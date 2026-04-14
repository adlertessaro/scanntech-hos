# componente responsável por processar as solicitações de auditoria e reset vindas da API, garantindo que as vendas e fechamentos sejam reenviados corretamente.
import logging
from api.auditoria import consultar_solicitacoes_vendas, consultar_solicitacoes_fechamentos

def executar_auditoria_e_reset(cur, config_loja):
    empresa_erp = int(config_loja['empresa'])
    
    # --- TRATAR VENDAS (MOVIMIENTOS) ---
    res_vendas = consultar_solicitacoes_vendas(config_loja)
    if res_vendas.get('sucesso') and res_vendas.get('dados'):
        for sol in res_vendas['dados']:
            estacao_reenvio = str(sol['codigoCaja']) if sol.get('codigoCaja') else None

            sql_reset_log = """
                UPDATE int_scanntech_vendas_logs
                SET id_lote = NULL
                WHERE empresa = %s AND data_registro = %s
            """
            params_reset = [empresa_erp, sol['fecha']]

            if estacao_reenvio:
                sql_reset_log += " AND estacao = %s"
                params_reset.append(estacao_reenvio)

            cur.execute(sql_reset_log, tuple(params_reset))
            _reinserir_fila_pelo_log(cur, empresa_erp, sol['fecha'], estacao_reenvio)
            logging.warning(f"♻️ Reset de VENDAS para data {sol['fecha']} PDV: {sol.get('codigoCaja', 'TODOS')} solicitado pela API.")

    # --- TRATAR FECHAMENTOS (CIERRES_DIARIOS) ---
    res_fech = consultar_solicitacoes_fechamentos(config_loja)
    if res_fech.get('sucesso') and res_fech.get('dados'):
        for sol in res_fech['dados']:
            # Reseta o status para o integrador pegar o registro de novo no próximo ciclo
            sql = "UPDATE int_scanntech_fechamentos SET id_lote = NULL, tentativas = 0 WHERE empresa = %s AND data_fechamento = %s"
            params = [empresa_erp, sol['fecha']]
            if sol.get('codigoCaja'):
                sql += " AND estacao = %s"
                params.append(str(sol['codigoCaja']))
            
            cur.execute(sql, tuple(params))
            logging.warning(f"♻️ Reset de FECHAMENTO para {sol['fecha']} PDV: {sol.get('codigoCaja', 'TODOS')}")

def _reinserir_fila_pelo_log(cur, empresa_erp, data, estacao=None):
    """
    Reinsere na fila de envio todas as vendas que tiveram id_lote nulificado,
    consultando diretamente o log — sem depender da trigger da caixa.
    """
    sql = """
        SELECT l.venda, l.estacao
        FROM int_scanntech_vendas_logs l
        WHERE l.empresa = %s
          AND l.data_registro = %s
          AND l.id_lote IS NULL
    """
    params = [empresa_erp, data]

    if estacao:
        sql += " AND l.estacao = %s"
        params.append(str(estacao))

    cur.execute(sql, tuple(params))
    registros = cur.fetchall()

    for (venda, estacao_log) in registros:
        cur.execute("""
            INSERT INTO int_scanntech_vendas
                (venda, empresa, estacao, tentativas, data_hora_inclusao)
            VALUES (%s, %s, %s, 0, CURRENT_TIMESTAMP)
            ON CONFLICT (venda, empresa) DO UPDATE
            SET tentativas = 0,
                data_hora_inclusao = CURRENT_TIMESTAMP
        """, (venda, empresa_erp, estacao_log))

    logging.info(f"📥 {len(registros)} vendas reinseridas na fila para reenvio (data: {data}).")