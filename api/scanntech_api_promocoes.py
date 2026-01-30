import logging
from scanntech.api import autenticacao
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def consultar_promocoes(config, loja_info, estado='ACEPTADA'):
    """
    Consulta a API de promoções da Scanntech para uma loja específica.
    """
    endpoint_path = f"/pmkt-rest-api/v2/minoristas/{loja_info['idEmpresa']}/locales/{loja_info['idLocal']}/promociones"
    
    tipos_desejados = ['LLEVA_PAGA', 'PRECIO_FIJO', 'DESCUENTO_VARIABLE', 'DESCUENTO_FIJO']
    params = { "estado": estado, "tipo": ",".join(tipos_desejados) }

    resposta = autenticacao.fazer_requisicao(config, endpoint_path, metodo='GET', params=params)

    if resposta:
        resposta_formatada = json.dumps(resposta, indent=2, ensure_ascii=False)
        logging.info(f"JSON de resposta de promoções para a loja {loja_info['idLocal']}:\n{resposta_formatada}")

    return resposta