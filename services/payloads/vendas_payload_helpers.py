# scanntech/services/payloads/vendas_payload_helpers.py
"""
Funções utilitárias puras usadas na montagem do payload de vendas.
Sem acesso ao banco de dados.
"""

import unicodedata

CANAIS_VENDA = {
    1: "VENDA NA LOJA", 2: "E-COMMERCE", 3: "IFOOD",
    4: "RAPPI", 6: "OUTROS", 8: "GLOVO",
}

FINALIZADORAS = {
    1: "PIX", 9: "DINHEIRO", 10: "CARTÃO CREDITO", 11: "CHEQUE",
    12: "OUTROS", 13: "CARTÃO DEBITO", 15: "FINALIZADORA",
}


def limpar_codigo_barras(cod_barra):
    if cod_barra is None:
        return ""
    if isinstance(cod_barra, float):
        return str(int(cod_barra))
    if isinstance(cod_barra, str) and cod_barra.endswith('.0'):
        return cod_barra[:-2]
    return str(cod_barra)


def remove_acentos(texto):
    if not texto:
        return "Nao Informado"
    return ''.join(
        c for c in unicodedata.normalize('NFD', str(texto))
        if unicodedata.category(c) != 'Mn'
    )


def converter_para_float(valor):
    """Converte valores do banco para float, retornando 0 se inválido."""
    try:
        return float(valor) if valor and valor != 'NENHUM' else 0.0
    except (ValueError, TypeError):
        return 0.0