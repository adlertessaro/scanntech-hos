# componente responsável por processar as solicitações de auditoria e reset vindas da API, garantindo que as vendas e fechamentos sejam reenviados corretamente.
import logging
from scanntech.api.auditoria import consultar_solicitacoes_vendas, consultar_solicitacoes_fechamentos

def executar_auditoria_e_reset(cur, config_loja):
    empresa_erp = int(config_loja['empresa'])
    
    # --- TRATAR VENDAS (MOVIMIENTOS) ---
    res_vendas = consultar_solicitacoes_vendas(config_loja)
    if res_vendas.get('sucesso') and res_vendas.get('dados'):
        for sol in res_vendas['dados']:
            # Se não houver codigoCaja, reenvia todas as caixas daquela data [cite: 615, 633, 646]
            sql_del_log = "DELETE FROM int_scanntech_vendas_logs WHERE empresa = %s AND data_registro = %s"
            sql_trigger = "UPDATE caixa SET venda = venda WHERE data = %s AND empresa = %s"
            params_del_log = [empresa_erp, sol['fecha']]
            params_trigger = [sol['fecha'], empresa_erp]
            if sol.get('codigoCaja'):
                sql_del_log += " AND estacao = %s"
                sql_trigger += " AND estacao = %s"
                params_del_log.append(str(sol['codigoCaja']))
                params_trigger.append(str(sol['codigoCaja']))
                
            cur.execute(sql_del_log, tuple(params_del_log))
            cur.execute(sql_trigger, tuple(params_trigger))
            logging.warning(f"♻️ Reset de VENDAS para data {sol['fecha']} solicitado pela API.")

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