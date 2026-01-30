# scanntech/models/gerar_fechamentos_pendentes.py

from datetime import datetime, timedelta
from scanntech.db.conexao import conectar
# Adicionado para carregar todas as configurações de lojas
from scanntech.config.settings import carregar_configuracoes
import logging

def gerar_fechamentos_pendentes(dias_retroativos=7):
    """
    Verifica e cria registros de fechamentos pendentes para TODAS as lojas configuradas,
    considerando os últimos N dias.
    """
    logging.info(f"Iniciando geração de fechamentos pendentes para os últimos {dias_retroativos} dias.")
    
    try:
        # Carrega a configuração completa para obter a lista de todas as lojas
        configs = carregar_configuracoes()
        lojas = configs.get('lojas', [])

        if not lojas:
            logging.warning("Nenhuma loja configurada. Encerrando geração de fechamentos.")
            return

        conn = conectar()
        cur = conn.cursor()

        # Itera sobre cada loja configurada
        for loja_config in lojas:
            empresa = loja_config.get("empresa")
            if not empresa:
                logging.warning(f"Loja com configuração incompleta (sem 'empresa'), pulando: {loja_config}")
                continue
            
            empresa = int(empresa)
            logging.info(f"\n--- Verificando fechamentos para a Empresa ERP: {empresa} ---")

            hoje = datetime.now().date()
            data_inicio = hoje - timedelta(days=dias_retroativos)
            data_atual = data_inicio

            while data_atual < hoje:
                # Busca estações com movimentação na data para a empresa atual
                cur.execute(
                    """
                    SELECT DISTINCT estacao FROM caixa
                    WHERE empresa = %s AND data = %s AND lancamen IN ('VV', 'VP', 'VC', 'VR', 'CC', 'DV')
                    """,
                    (empresa, data_atual),
                )
                estacoes_com_movimento = [row[0] for row in cur.fetchall()]

                if not estacoes_com_movimento:
                    logging.info(f"Sem movimentação em {data_atual.strftime('%d/%m/%Y')} para a Empresa {empresa}.")
                else:
                    for estacao in estacoes_com_movimento:
                        # Verifica a existência do fechamento para a estação específica
                        cur.execute(
                            """
                            SELECT 1 FROM int_scanntech_fechamentos
                            WHERE empresa = %s AND data_fechamento = %s AND estacao = %s
                            """,
                            (empresa, data_atual, estacao),
                        )

                        if cur.fetchone() is None:
                            # Insere o fechamento pendente para a estação
                            cur.execute(
                                """
                                INSERT INTO int_scanntech_fechamentos (data_fechamento, empresa, estacao, tentativas, data_hora_inclusao)
                                VALUES (%s, %s, %s, 0, %s)
                                """,
                                (data_atual, empresa, estacao, datetime.now()),
                            )
                            logging.info(f"✅ Fechamento gerado para {data_atual.strftime('%d/%m/%Y')} (Estação {estacao})")
                
                data_atual += timedelta(days=1)

        conn.commit()
        cur.close()
        conn.close()
        logging.info("\n✅ Geração de fechamentos pendentes finalizada para todas as lojas.")

    except Exception as e:
        logging.error(f"❌ Erro ao gerar fechamentos pendentes: {e}", exc_info=True)