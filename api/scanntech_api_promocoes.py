from scanntech.api import autenticacao

def consultar_promocoes(config, estado='ACEPTADA'):
    endpoint_path = "/pmkt-rest-api/v2/minoristas/{idEmpresa}/locales/{idLocal}/promociones"
    tipos_desejados = ['LLEVA_PAGA', 'PRECIO_FIJO', 'DESCUENTO_VARIABLE', 'DESCUENTO_FIJO']
    params = {
        "estado": estado,
        "tipo": ",".join(tipos_desejados)  # Tenta filtrar por tipo na API
    }
    return autenticacao.fazer_requisicao(config, endpoint_path, metodo='GET', params=params)

# def consultar_promocoes_limite_ticket(config, estado='ACEPTADA'):
#     endpoint_path = "/pmkt-rest-api/minoristas/{idEmpresa}/locales/{idLocal}/promocionesConLimitePorTicket"
#     params = {"estado": estado}
#     return autenticacao.fazer_requisicao(config, endpoint_path, metodo='GET', params=params)

# def consultar_promocoes_crm(config, estado='ACEPTADA'):
#     endpoint_path = "/pmkt-rest-api/v3/minoristas/{idEmpresa}/locales/{idLocal}/promociones-crm"
#     params = {"estado": estado}
#     return autenticacao.fazer_requisicao(config, endpoint_path, metodo='GET', params=params)