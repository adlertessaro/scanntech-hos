import sys
import os
from datetime import datetime

# Adiciona o caminho absoluto da pasta raiz do projeto ao sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from scanntech.config.settings import carregar_configuracoes
from scanntech.services.payloads.fechamentos_payload import montar_payload_do_fechamento
from scanntech.api.scanntech_api_fechamentos import enviar_fechamentos_lote
from scanntech.db.conexao import conectar
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def processar_envio_fechamento(config):
    """
    Processa e envia um fechamento CONSOLIDADO para cada dia pendente.
    """
    conn = None
    try:
        empresa_configurada = config.get("empresa")
        if not empresa_configurada:
            logging.warning("Nenhuma empresa configurada. Encerrando processamento.")
            return

        conn = conectar()
        cur = conn.cursor()

        # 1. Busca os dias pendentes da tabela de controle (sem agrupar por caixa)
        cur.execute("""
            SELECT empresa, data_fechamento
            FROM int_scanntech_fechamentos
            WHERE tentativas < 3 AND id_lote IS NULL AND empresa = %s
            GROUP BY empresa, data_fechamento
        """, (empresa_configurada,))
        dias_a_processar = cur.fetchall()
        
        logging.info(f"Encontrados {len(dias_a_processar)} dias de fechamento pendentes: {dias_a_processar}")

        if not dias_a_processar:
            return

        # 2. Itera sobre cada dia e envia um único fechamento consolidado
        for empresa, data_fechamento in dias_a_processar:
            logging.info(f"\n--- Processando fechamento CONSOLIDADO para o dia: {data_fechamento.strftime('%Y-%m-%d')} ---")
            
            try:
                # Chama a função SEM o id_caixa para obter o total do dia
                payload_consolidado = montar_payload_do_fechamento(empresa, config, data_fechamento)

                if not payload_consolidado:
                    logging.warning(f"Nenhum movimento encontrado para o dia {data_fechamento}. Marcando como processado.")
                    cur.execute("""
                        UPDATE int_scanntech_fechamentos
                        SET erro = 'Sem movimentos neste dia', tentativas = 3, data_hora_tentativa = %s
                        WHERE empresa = %s AND data_fechamento = %s
                    """, (datetime.now(), empresa, data_fechamento))
                    continue

                # Para a API, usamos um ID de caixa padrão da configuração
                id_caixa_para_api = config.get("local") or "1"
                logging.info(f"Enviando para API sob o id_caixa padrão: {id_caixa_para_api}")
                
                resposta = enviar_fechamentos_lote(config, id_caixa_para_api, payload_consolidado)
                
                status = resposta.get("status_code")
                dados = resposta.get("dados")

                if status == 200 and not dados.get("errores"):
                    id_lote = dados.get("idLote", "enviado")
                    logging.info(f"✅ SUCESSO para o dia {data_fechamento}. Lote: {id_lote}")
                    cur.execute("""
                        UPDATE int_scanntech_fechamentos
                        SET id_lote = %s, erro = NULL, data_hora_tentativa = %s
                        WHERE empresa = %s AND data_fechamento = %s
                    """, (id_lote, datetime.now(), empresa, data_fechamento))
                else:
                    erro_msg = resposta.get("mensagem", str(dados.get("errores", "Erro desconhecido")))
                    logging.error(f"❌ FALHA para o dia {data_fechamento}. Erro: {erro_msg}")
                    cur.execute("""
                        UPDATE int_scanntech_fechamentos
                        SET tentativas = tentativas + 1, erro = %s, data_hora_tentativa = %s
                        WHERE empresa = %s AND data_fechamento = %s
                    """, (erro_msg[:255], datetime.now(), empresa, data_fechamento))

            except Exception as e_interno:
                logging.error(f"Erro crítico ao processar o dia {data_fechamento}: {e_interno}", exc_info=True)
        
        conn.commit()

    except Exception as e_geral:
        logging.error(f"Erro geral no processador de fechamentos: {e_geral}", exc_info=True)
    finally:
        if conn and not getattr(conn, 'closed', True):
            cur.close()
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")


if __name__ == "__main__":
    configs = carregar_configuracoes()
    processar_envio_fechamento(configs)