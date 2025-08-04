import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from datetime import datetime, timedelta
from scanntech.config.settings import carregar_configuracoes
from scanntech.utils.logger import configurar_saida_terminal
from scanntech.services.promocoes_service import processar_promocoes
from scanntech.services.vendas_service import processar_envio_vendas
from scanntech.services.fechamentos_service import processar_envio_fechamento
from scanntech.models.gerar_fechamentos_pendentes import gerar_fechamentos_pendentes
from scanntech.db.promo_repo import reprocessar_produtos_pendentes

def limitar_codigo_caixa(id_caja):
    id_caja_str = str(id_caja)
    resultado = id_caja_str[-5:] if len(id_caja_str) > 5 else id_caja_str.zfill(5)
    print(f"DEBUG: id_caja original: {id_caja}, id_caja limitado: {resultado}")
    return resultado

def iniciar_loop():
    configurar_saida_terminal()

    try:
        config = carregar_configuracoes()
        integracao_habilitada = config.get("integração scanntech", "true").lower() == "true"

        if not integracao_habilitada:
            print("⚠️ Integração Scanntech está desativada. Encerrando loop.")
            return

        if 'caja' in config:
            config['caja'] = limitar_codigo_caixa(config['caja'])

        intervalo_str = config.get("intervalo (s)")
        intervalo = int(intervalo_str) if intervalo_str and intervalo_str.isdigit() else 30

        print(f"🔄 Iniciando integrador. Intervalo definido: {intervalo} segundos.\n")
        print("🧪 Verificando configuração inicial...\n")

        # Gera fechamentos pendentes dos últimos 7 dias logo na inicialização
        print("🚀 Verificando fechamentos pendentes dos últimos 7 dias ao iniciar...")
        gerar_fechamentos_pendentes(config, dias_retroativos=7)

        # ⏱️ Controle interno de tempo
        ultimo_promocoes = None
        ultimo_fechamento_diario = None

        while True:
            agora = datetime.now()

            # PROMOÇÕES — a cada 30 minutos
            if not ultimo_promocoes or (agora - ultimo_promocoes).total_seconds() >= 1800:
                try:
                    print("🚀 Processando promoções...")
                    processar_promocoes(config)
                    print("🔁 Reprocessando produtos pendentes...")
                    reprocessar_produtos_pendentes()
                    ultimo_promocoes = datetime.now()
                except Exception as e:
                    print(f"❌ Erro ao processar promoções: {e}")

            # VENDAS — SEMPRE que houver
            try:
                print("🚀 Processando envios de vendas...")
                processar_envio_vendas(config)
            except Exception as e:
                print(f"❌ Erro ao processar vendas: {e}")

            # GERAÇÃO DE FECHAMENTOS — às 02h (1x por dia)
            if agora.hour == 2 and agora.minute < 5:
                if not ultimo_fechamento_diario or ultimo_fechamento_diario.date() != agora.date():
                    try:
                        print("📅 Verificando fechamentos pendentes dos últimos 7 dias...")
                        gerar_fechamentos_pendentes(config, dias_retroativos=7)
                        ultimo_fechamento_diario = datetime.now()
                    except Exception as e:
                        print(f"❌ Erro ao gerar fechamentos pendentes: {e}")

            # ENVIO DE FECHAMENTOS — SEMPRE que houver
            try:
                print("🚀 Processando envios de fechamentos...")
                processar_envio_fechamento(config)
            except Exception as e:
                print(f"❌ Erro ao processar fechamentos: {e}")

            print(f"⏳ Aguardando próximo ciclo ({intervalo} segundos)...")
            time.sleep(intervalo)

    except Exception as erro_geral:
        print(f"❌ Erro ao iniciar o integrador: {erro_geral}")
        raise

if __name__ == "__main__":
    iniciar_loop()
