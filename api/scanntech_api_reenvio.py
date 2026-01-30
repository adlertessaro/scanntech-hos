# scanntech/api/scanntech_api_reenvio.py
from . import autenticacao

def consultar_solicitacoes_vendas(config):
    """
    Consulta a API para obter dias nos quais é necessário reenviar as vendas.
    """
    endpoint_path = "/api-minoristas/api/v2/minoristas/{idEmpresa}/locales/{idLocal}/solicitudes/movimientos"
    return autenticacao.fazer_requisicao(config, endpoint_path, metodo='GET')

def consultar_solicitacoes_fechamentos(config):
    """
    Consulta a API para obter dias nos quais é necessário reenviar os fechamentos.
    """
    endpoint_path = "/api-minoristas/api/v2/minoristas/{idEmpresa}/locales/{idLocal}/solicitudes/cierresDiarios"
    return autenticacao.fazer_requisicao(config, endpoint_path, metodo='GET')