import threading
import time
import logging
from datetime import datetime
from scanntech.config.settings import carregar_configuracoes
from scanntech.utils.logger import configurar_logger
from scanntech.services.promocoes_service import processar_promocoes
from scanntech.services.vendas_service import processar_envio_vendas
from scanntech.services.processors.fechamentos_processor import processar_envio_fechamento
from scanntech.models.gerar_fechamentos_pendentes import gerar_fechamentos_pendentes
from scanntech.db.promo_repo import salvar_e_processar_promocoes
from scanntech.api.auditoria import consultar_solicitacoes_vendas, consultar_solicitacoes_fechamentos
from scanntech.db.conexao import conectar
from scanntech.services.processors.auditoria_processor import executar_auditoria_e_reset, reinserir_cancelamentos_pendentes
from scanntech.services.processors.fechamentos_processor import invalidar_fechamentos_desatualizados


class IntegradorLoop:
    """
    Encapsula toda a lógica do loop do integrador em uma classe,
    gerenciando seu próprio estado e ciclo de vida.
    """
    def __init__(self, parar_evento: threading.Event):
        self.parar_evento = parar_evento
        self.configs = {}
        self.intervalo = 1800  # Valor padrão
        self.ultimo_ciclo_promocoes = None
        self.ultimo_ciclo_fechamento_diario = None
        configurar_logger()
        self.ultimo_ciclo_auditoria = None

    def _ciclo_auditoria(self, agora: datetime):
        if self.ultimo_ciclo_auditoria and (agora - self.ultimo_ciclo_auditoria).total_seconds() < 43200:
            return

        logging.info("🕵️ Iniciando auditoria de reenvios (Ciclo 12h)...")

        conn = None
        houve_reenvio = False
        try:
            conn = conectar()
            cur = conn.cursor()
            for loja in self.configs.get('lojas', []):
                config_loja = {**self.configs.get('geral', {}), **loja}
                houve_reenvio |= executar_auditoria_e_reset(cur, config_loja)
            conn.commit()
            self.ultimo_ciclo_auditoria = agora
        except Exception as e:
            logging.error(f"❌ Erro na auditoria: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

        if houve_reenvio:
            logging.info("🔄 Reenvio detectado: enviando VENDAs pendentes...")
            self._ciclo_vendas()

            logging.info("🔄 Reinserindo cancelamentos pendentes na fila...")
            conn = None
            try:
                conn = conectar()
                cur = conn.cursor()
                for loja in self.configs.get('lojas', []):
                    config_loja = {**self.configs.get('geral', {}), **loja}
                    reinserir_cancelamentos_pendentes(cur, config_loja)
                conn.commit()
            except Exception as e:
                logging.error(f"❌ Erro ao reinserir cancelamentos: {e}")
                if conn: conn.rollback()
            finally:
                if conn: conn.close()

            logging.info("🔄 Enviando cancelamentos pendentes...")
            self._ciclo_vendas()

    def _carregar_e_validar_configs(self):
        """Carrega as configurações e verifica se a integração está ativa."""
        self.configs = carregar_configuracoes()
        config_geral = self.configs.get('geral', {})
        
        if not config_geral.get("habilitar_integracao_scanntech", "true").lower() == "true":
            logging.warning("⚠️ Integração Scanntech está desativada nas configurações. Encerrando.")
            return False

        intervalo_str = config_geral.get("intervalo_s", "1800")
        self.intervalo = int(intervalo_str) if intervalo_str.isdigit() else 1800
        return True
    
    def _desativar_carga_inicial(self):
        """Desativa o switch de carga inicial no arquivo físico após a execução."""
        from configparser import ConfigParser
        from config.settings import CONFIG_PATH

        config_geral = self.configs.get('geral', {})
        if config_geral.get("carga_inicial", "false").lower() == "true":
            logging.info("🎯 Ciclo de carga finalizado. Desativando switch 'carga_inicial'...")
            parser = ConfigParser()
            parser.read(CONFIG_PATH, encoding="utf-8")
            if parser.has_section("SCANNTECH_GERAL"):
                parser.set("SCANNTECH_GERAL", "carga_inicial", "false")
                with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                    parser.write(f)
            # Recarrega as configurações para o próximo ciclo já vir como 'false'
            self.configs = carregar_configuracoes()

    def _ciclo_promocoes(self, agora: datetime):
        """Executa a lógica de busca e salvamento de promoções."""
        if self.ultimo_ciclo_promocoes and (agora - self.ultimo_ciclo_promocoes).total_seconds() < 1800:
            return # Ainda não é hora de rodar as promoções

        logging.info("🚀 Processando promoções para TODAS as lojas...")
        try:
            # Recarregar configs pode ser útil se elas mudam dinamicamente
            configs_atualizadas = carregar_configuracoes() 
            config_geral_atual = configs_atualizadas.get('geral', {})
            todas_as_lojas = configs_atualizadas.get('lojas', [])
            
            if not todas_as_lojas:
                logging.warning("⚠️ Nenhuma loja configurada para buscar promoções.")
                return

            promocoes_para_salvar = {}
            for loja in todas_as_lojas:

                empresa_id = loja.get('empresa') # Guardar ID para o log

                try:
                    logging.info(f"  -> Buscando para loja com ERP ID: {loja.get('empresa')}")
                    config_completa = {**config_geral_atual, **loja}

                    logging.debug(f"🔍 [DEBUG] Iniciando requisição de promoções para loja {empresa_id}")

                    resultado = processar_promocoes(config_completa)
                    if resultado:
                        qtd = len(resultado) if isinstance(resultado, (list, dict)) else "N/A"
                        logging.debug(f"✅ [DEBUG] Recebido {qtd} promoções da loja {empresa_id}")
                        promocoes_para_salvar.update(resultado)
                    else:
                        logging.debug(f"❌ [DEBUG] Nenhuma promoção encontrada para a loja {empresa_id}")

                except Exception as e:
                    logging.error(f"❌ Erro ao buscar promoções para a loja {loja.get('empresa')}: {e}")

            if promocoes_para_salvar:
                logging.info(f"\n💾 Salvando promoções de {len(promocoes_para_salvar)} loja(s) no banco de dados...")
                salvar_e_processar_promocoes(promocoes_para_salvar)
            else:
                logging.info("✅ Nenhuma promoção nova encontrada.")

        except Exception as e:
            logging.error(f"❌ Erro geral ao processar promoções: {e}")

    def _ciclo_vendas(self):
        """Executa a lógica de envio de vendas."""
        logging.info("🚀 Processando envios de vendas...")
        try:
            processar_envio_vendas()
        except Exception as e:
            logging.error(f"❌ Erro ao processar vendas: {e}")

    def _ciclo_envio_fechamentos(self):
        """Executa a lógica de envio de fechamentos pendentes."""
        logging.info("🚀 Processando envios de fechamentos...")
        try:
            logging.debug("📤 [DEBUG] Iniciando ciclo de envio de fechamentos...")

            # 1. Carrega as configurações ATUAIS (para garantir que temos os dados frescos)
            configs = carregar_configuracoes()
            config_geral = configs.get('geral', {})
            lojas = configs.get('lojas', [])

            if not lojas:
                logging.warning("⚠️ Nenhuma loja configurada para enviar fechamentos.")
                return

            # 2. Itera sobre cada loja e passa os dados para o processador
            # Isso resolve o problema do "settings.config não encontrado" dentro da função
            for loja in lojas:
                empresa = int(loja.get('empresa'))
                config_completa = {**config_geral, **loja}

                logging.info(f"--- Verificando fechamentos para a Empresa ERP: {empresa} ---")
                
                processar_envio_fechamento(empresa, config_completa)

            logging.debug("🏁 [DEBUG] Ciclo de fechamentos finalizado.")
            
        except Exception as e:
            logging.error(f"❌ Erro ao processar fechamentos: {e}")

    def _ciclo_fechamentos(self, agora: datetime):
        """Verifica e gera fechamentos pendentes, uma vez por dia."""
        # Roda apenas uma vez por dia, entre 02:00 e 02:05 da manhã
        if agora.hour == 2 and agora.minute < 5:
            if not self.ultimo_ciclo_fechamento_diario or self.ultimo_ciclo_fechamento_diario.date() != agora.date():
                logging.info("📅 Verificando fechamentos pendentes dos últimos 7 dias...")
                try:
                    gerar_fechamentos_pendentes(dias_retroativos=7)
                    self._invalidar_fechamentos_todas_lojas()
                    self.ultimo_ciclo_fechamento_diario = datetime.now()
                except Exception as e:
                    logging.error(f"❌ Erro ao gerar fechamentos pendentes: {e}")

    def _invalidar_fechamentos_todas_lojas(self):
        """Invalida fechamentos com divergência, uma vez por dia."""
        conn = None
        try:
            conn = conectar()
            cur = conn.cursor()
            for loja in self.configs.get('lojas', []):
                empresa_erp = int(loja.get('empresa'))
                invalidar_fechamentos_desatualizados(cur, empresa_erp)
            conn.commit()
        except Exception as e:
            logging.error(f"❌ Erro ao invalidar fechamentos: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def iniciar(self):
        """O ponto de entrada principal que executa o loop infinito."""
        logging.info("--- Iniciando Integrador Scanntech ---")

        if not self._carregar_e_validar_configs():
            return
        if not self._carregar_e_validar_configs():
            return

        logging.info(f"🔄 Loop iniciado. Intervalo entre ciclos: {self.intervalo} segundos.")
        logging.info("🧪 Verificando fechamentos pendentes (últimos 7 dias) ao iniciar...")
        try:
            gerar_fechamentos_pendentes(dias_retroativos=7)
        except Exception as e:
            logging.error(f"❌ Erro ao gerar fechamentos na inicialização: {e}")
        
        self.ultimo_ciclo_promocoes = datetime.now() # Marcar o tempo inicial para promoções

        # Loop principal
        while not self.parar_evento.is_set():

            # 🔒 VERIFICAÇÃO DE LICENÇA
            from scanntech.api.license import is_blocked
            if is_blocked():
                logging.warning("🚫 Sistema não liberado. Entre em contato com o administrador.")
                self.parar_evento.wait(self.intervalo)  # Espera o intervalo antes de verificar novamente
                continue
            else:
                logging.info("✅ Licença verificada: sistema autorizado para operar.")

            agora = datetime.now()
            self._ciclo_auditoria(agora)
            self._ciclo_promocoes(agora)
            self._ciclo_vendas()
            self._ciclo_envio_fechamentos()
            self._ciclo_fechamentos(agora)
            self._desativar_carga_inicial()

            logging.info(f"⏳ Ciclo concluído. Aguardando {self.intervalo} segundos...")
            # O 'wait' é interrompido se o evento 'parar_evento' for acionado
            self.parar_evento.wait(self.intervalo)

        logging.info("--- Integrador Scanntech Encerrado ---")


if __name__ == "__main__":
    # Este bloco é útil para testar o loop.py diretamente
    print("Executando loop.py em modo de teste direto. Pressione Ctrl+C para parar.")
    evento_parada_teste = threading.Event()
    
    integrador = IntegradorLoop(evento_parada_teste)
    
    try:
        integrador.iniciar()
    except KeyboardInterrupt:
        print("\nComando de parada recebido. Encerrando...")
        evento_parada_teste.set()