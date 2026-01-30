from scanntech.api.scanntech_api_promocoes import consultar_promocoes as get_promocoes
from scanntech.db.promo_repo import salvar_e_processar_promocoes
import logging

def processar_promocoes(config_completa):
    """
    Busca promoções para uma única loja, recebendo uma configuração unificada
    que contém tanto os dados gerais (autenticação) quanto os da loja.
    """
    # Extrai os dados da loja do dicionário de configuração completo
    id_empresa = config_completa.get("idempresa")
    id_local = config_completa.get("idlocal")
    empresa_erp = config_completa.get("empresa")
    
    if not all([id_empresa, id_local, empresa_erp]):
        logging.error(f"Configuração da loja incompleta: {config_completa}")
        raise ValueError("Configuração de loja inválida.")

    logging.info(f"Buscando promoções para a loja: idLocal={id_local}, idEmpresa={id_empresa}")
    
    loja_info = {
        "idEmpresa": id_empresa,
        "idLocal": id_local,
        "empresaErp": int(empresa_erp)
    }

    # A função `get_promocoes` agora recebe o dicionário completo, que contém
    # as credenciais de autenticação e qualquer outro parâmetro geral.
    resposta = get_promocoes(config_completa, loja_info)
    
    if resposta and resposta.get("status_code") == 200:
        promocoes = resposta.get("dados", {}).get("results", [])
        if promocoes:
            logging.info(f"{len(promocoes)} promoções encontradas para a loja {id_local}.")
            return {loja_info['empresaErp']: promocoes}
        else:
            logging.info(f"Nenhuma promoção encontrada para a loja {id_local}.")
            return {loja_info['empresaErp']: []}
    else:
        status_code = resposta.get("status_code") if resposta else "N/A"
        msg_erro = f"Erro ao buscar promoções para a loja {id_local}. Código: {status_code}"
        logging.error(msg_erro)
        raise ConnectionError(msg_erro)