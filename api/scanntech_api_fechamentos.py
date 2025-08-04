from api import autenticacao

def enviar_fechamentos_lote(config, id_caja, fechamentos):
    id_caja = int(id_caja)

    endpoint_path = f"/api-minoristas/api/v2/minoristas/{{idEmpresa}}/locales/{{idLocal}}/cajas/{id_caja}/cierresDiarios/lotes"

    return autenticacao.fazer_requisicao(config=config, endpoint_path=endpoint_path, metodo='POST', dados=fechamentos)