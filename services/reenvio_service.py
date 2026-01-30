# scanntech/services/reenvio_service.py

from scanntech.api.scanntech_api_reenvio import consultar_solicitacoes_vendas, consultar_solicitacoes_fechamentos
from scanntech.db.vendas_repo import marcar_vendas_para_reenvio
from scanntech.db.fechamentos_repo import marcar_fechamentos_para_reenvio
# Adicionado para carregar as configurações de todas as lojas
from scanntech.config.settings import carregar_configuracoes
import logging

def verificar_e_processar_reenvios():
    """
    Verifica e processa solicitações de reenvio para vendas e fechamentos de TODAS as lojas configuradas.
    Retorna uma mensagem de resumo.
    """
    logging.info("Iniciando verificação de solicitações de reenvio para todas as lojas...")
    
    configs = carregar_configuracoes()
    config_geral = configs.get('geral', {})
    lojas = configs.get('lojas', [])
    
    if not lojas:
        logging.warning("Nenhuma loja configurada para verificar reenvios.")
        return "Nenhuma loja configurada."

    resumo_geral = []

    # Itera sobre cada loja configurada
    for loja_config in lojas:
        empresa_erp = loja_config.get("empresa")
        id_local = loja_config.get("idlocal")
        if not empresa_erp or not id_local:
            continue

        logging.info(f"\n--- Verificando reenvios para a Loja {id_local} (Empresa ERP: {empresa_erp}) ---")
        config_completa_loja = {**config_geral, **loja_config}
        
        # Processa Vendas para a loja atual
        try:
            resposta_vendas = consultar_solicitacoes_vendas(config_completa_loja)
            if resposta_vendas.get("status_code") == 200:
                solicitacoes = resposta_vendas.get("dados", [])
                if solicitacoes:
                    total = marcar_vendas_para_reenvio(solicitacoes, empresa_erp)
                    resumo_geral.append(f"Loja {id_local}: {total} dias/caixas de vendas marcados para reenvio.")
                else:
                    logging.info(f"Nenhuma solicitação de reenvio de vendas para a loja {id_local}.")
            else:
                resumo_geral.append(f"Loja {id_local}: Erro ao consultar reenvio de vendas (Código: {resposta_vendas.get('status_code')}).")
        except Exception as e:
            logging.error(f"Erro ao processar reenvio de vendas para loja {id_local}: {e}")

        # Processa Fechamentos para a loja atual
        try:
            resposta_fechamentos = consultar_solicitacoes_fechamentos(config_completa_loja)
            if resposta_fechamentos.get("status_code") == 200:
                solicitacoes = resposta_fechamentos.get("dados", [])
                if solicitacoes:
                    total = marcar_fechamentos_para_reenvio(solicitacoes, empresa_erp)
                    resumo_geral.append(f"Loja {id_local}: {total} dias/caixas de fechamentos marcados para reenvio.")
                else:
                    logging.info(f"Nenhuma solicitação de reenvio de fechamentos para a loja {id_local}.")
            else:
                resumo_geral.append(f"Loja {id_local}: Erro ao consultar reenvio de fechamentos (Código: {resposta_fechamentos.get('status_code')}).")
        except Exception as e:
            logging.error(f"Erro ao processar reenvio de fechamentos para loja {id_local}: {e}")

    if not resumo_geral:
        return "Verificação de reenvios concluída. Nenhuma ação necessária."
        
    return "\n".join(resumo_geral)