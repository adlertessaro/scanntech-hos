from api import autenticacao

def enviar_fechamentos_lote(config, id_caja, fechamentos):
    """
    Envia um lote de fechamentos diários para a API da Scanntech.
    """
    id_empresa = config.get('idempresa')
    id_local = config.get('idlocal')

    # Validação para garantir que os dados essenciais estão presentes
    if not id_empresa or not id_local:
        # Retornamos um dicionário de erro padronizado para ser tratado pelo processor
        return {
            "status_code": 400,
            "sucesso": False,
            "mensagem": "ID da Empresa ou ID do Local não encontrado nas configurações."
        }

    id_caja_int = int(id_caja)

    # Agora as variáveis estão definidas e podem ser usadas na f-string
    endpoint_path = f"/api-minoristas/api/v2/minoristas/{id_empresa}/locales/{id_local}/cajas/{id_caja_int}/cierresDiarios/lotes"

    return autenticacao.fazer_requisicao(config=config, endpoint_path=endpoint_path, metodo='POST', dados=fechamentos)