#componente utilizado para que a scanntech possa solicitar dados e não apenas o integrdor mandar conforme programado.
import logging
from scanntech.api import autenticacao
from datetime import datetime


def consultar_solicitacoes_vendas(config):
    """
    Consulta a API para obter dias nos quais é necessário reenviar as vendas. [cite: 616]
    Endpoint: /solicitudes/movimientos [cite: 611, 620]
    """
    id_empresa = config.get('idempresa')
    id_local = config.get('idlocal')
    
    endpoint_path = f"/api-minoristas/api/v2/minoristas/{id_empresa}/locales/{id_local}/solicitudes/movimientos"
    return autenticacao.fazer_requisicao(config, endpoint_path, metodo='GET')

def consultar_solicitacoes_fechamentos(config):
    """
    Consulta a API para obter dias nos quais é necessário reenviar os fechamentos.
    Endpoint: /solicitudes/cierresDiarios [cite: 611, 633]
    """
    id_empresa = config.get('idempresa')
    id_local = config.get('idlocal')
    
    endpoint_path = f"/api-minoristas/api/v2/minoristas/{id_empresa}/locales/{id_local}/solicitudes/cierresDiarios"
    return autenticacao.fazer_requisicao(config, endpoint_path, metodo='GET')