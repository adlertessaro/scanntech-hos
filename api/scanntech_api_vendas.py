import json
from scanntech.api import autenticacao

def validar_codigo_caixa(id_caja):
    """
    Valida que o código do caixa é uma string de 5 dígitos. Se inválido, retorna valor padrão.
    Exemplo: '68051' -> '68051', '123' -> '00123', 1168051.0 -> '68051'
    """
    try:
        # Converte float para inteiro, se necessário
        if isinstance(id_caja, float):
            id_caja = int(id_caja)
        # Converte para string e remove caracteres não numéricos
        id_caja_str = ''.join(c for c in str(id_caja) if c.isdigit())
        # Pega os últimos 5 dígitos ou preenche com zeros
        resultado = id_caja_str[-5:] if len(id_caja_str) > 5 else id_caja_str.zfill(5)
        return resultado
    except (TypeError, ValueError) as e:
        print(f"⚠️ Erro ao validar id_caja '{id_caja}': {e}")
        return "00001"  # Valor padrão seguro

def enviar_vendas_lote(config, id_caja, vendas):
    id_caja_validado = validar_codigo_caixa(id_caja)

    print("📤 Payload que será enviado:")
    print(json.dumps(vendas, indent=2, ensure_ascii=False))

    endpoint_path = f"/api-minoristas/api/v2/minoristas/{{idEmpresa}}/locales/{{idLocal}}/cajas/{id_caja_validado}/movimientos/lotes"


    resultado = autenticacao.fazer_requisicao(config, endpoint_path, metodo='POST', dados=vendas) 
    
    return resultado