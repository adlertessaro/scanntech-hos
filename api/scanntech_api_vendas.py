# scanntech/api/scanntech_api_vendas.py

import json
from scanntech.api import autenticacao

def validar_codigo_caixa(id_caja):
    """
    Valida que o código do caixa é uma string de 5 dígitos. Se inválido, retorna valor padrão.
    """
    try:
        if isinstance(id_caja, float):
            id_caja = int(id_caja)
        id_caja_str = ''.join(c for c in str(id_caja) if c.isdigit())
        resultado = id_caja_str[-5:] if len(id_caja_str) > 5 else id_caja_str.zfill(5)
        return resultado
    except (TypeError, ValueError) as e:
        print(f"⚠️ Erro ao validar id_caja '{id_caja}': {e}")
        return "00001"

# --- ALTERAÇÃO APLICADA AQUI ---
# A função agora recebe os IDs da empresa e local diretamente, em vez de depender do dicionário 'config'.
def enviar_vendas_lote(config, id_empresa_scanntech, id_local_scanntech, id_caja, vendas):
    """
    Envia um lote de vendas para a API da Scanntech, formatando o payload e a URL final.
    """
    id_caja_validado = validar_codigo_caixa(id_caja)
    
    # O payload da requisição agora é apenas a lista de vendas, conforme a documentação.
    # A API espera um array de movimentos, não um dicionário {"movimientos": [...]}.
    payload_final = vendas

    print("📤 Payload final que será enviado:")
    print(json.dumps(payload_final, indent=2, ensure_ascii=False))

    # Monta o caminho do endpoint usando os parâmetros recebidos diretamente.
    endpoint_path = f"/api-minoristas/api/v2/minoristas/{id_empresa_scanntech}/locales/{id_local_scanntech}/cajas/{id_caja_validado}/movimientos/lotes"

    # A função fazer_requisicao agora recebe o caminho já formatado corretamente.
    resultado = autenticacao.fazer_requisicao(config, endpoint_path, metodo='POST', dados=payload_final) 
    
    return resultado