import sys
import os
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
    
from db.conexao import conectar

def montar_payload_do_fechamento(empresa, config, data_fechamento, id_caixa=None):
    if id_caixa:
        logging.info(f"Montando payload para: Empresa={empresa}, Data={data_fechamento}, Caixa Específico={id_caixa}")
    else:
        logging.info(f"Montando payload CONSOLIDADO para: Empresa={empresa}, Data={data_fechamento}")
    
    conn = None
    try:
        conn = conectar()
        cur = conn.cursor()

        # --- CORREÇÃO PRINCIPAL AQUI ---
        # Formata o objeto de data para o formato de texto com pontos (YYYY.MM.DD)
        data_formatada_para_sql = data_fechamento.strftime('%Y.%m.%d')
        logging.info(f"Data formatada para a consulta SQL: '{data_formatada_para_sql}'")
        
        sql_base = """
            SELECT
                COALESCE(SUM(CASE WHEN lancamen IN ('VV', 'VP', 'VC', 'VR') THEN valor ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN lancamen IN ('CC') THEN valor ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN lancamen IN ('DV') THEN valor ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN lancamen IN ('VV', 'VP', 'VC', 'VR') THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN lancamen IN ('CC', 'DV') THEN 1 ELSE 0 END), 0)
            FROM caixa
            WHERE empresa = %s AND data = %s
        """
        # Usa a data já formatada como string nos parâmetros da consulta
        params = (empresa, data_formatada_para_sql)

        if id_caixa:
            sql_base += " AND caixa = %s"
            params += (str(id_caixa),)

        cur.execute(sql_base, params)
        resultado = cur.fetchone()

        if not resultado or (resultado[3] == 0 and resultado[4] == 0):
            logging.info("Sem movimentos no banco de dados para os critérios fornecidos.")
            return []

        valor_vendas_brutas, valor_cancelamentos, valor_devolucao, qtd_vendas, qtd_cancelamentos = resultado
        valor_liquido = valor_vendas_brutas - valor_devolucao
        valor_cc_dv = valor_devolucao + valor_cancelamentos
        total_movimentos_iniciados = qtd_vendas + qtd_cancelamentos

        payload = [{"fechaVentas": data_fechamento.strftime("%Y-%m-%d"), "montoVentaLiquida": round(float(valor_liquido), 2), "montoCancelaciones": round(float(valor_cc_dv), 2), "cantidadMovimientos": int(total_movimentos_iniciados), "cantidadCancelaciones": int(qtd_cancelamentos)}]
        logging.info(f"Payload final gerado: {payload}")
        return payload
    except Exception as e:
        logging.error(f"❌ ERRO ao montar payload do fechamento: {e}", exc_info=True)
        return []
    finally:
        if conn and not getattr(conn, 'closed', True):
            cur.close()
            conn.close()