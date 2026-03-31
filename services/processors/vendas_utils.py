# scanntech/services/processors/vendas_utils.py
"""
Funções utilitárias puras (sem acesso ao banco) para o processador de vendas.
"""

import logging
from datetime import datetime, timedelta


CODIGOS_ACEITOS = ['VV', 'VP', 'VC', 'CR', 'CH', 'CP', 'CC', 'DV', 'DP']
CODIGOS_DEVOLUCAO = ('CC', 'DV', 'DP')
CODIGOS_VENDA = ('VV', 'VP', 'VC', 'CR', 'CH', 'CP')


def limitar_codigo_estacao(estacao):
    try:
        if isinstance(estacao, float):
            estacao = int(estacao)
        estacao_str = ''.join(c for c in str(estacao) if c.isdigit())
        return estacao_str[-5:] if len(estacao_str) > 5 else estacao_str.zfill(5)
    except (TypeError, ValueError):
        return "00001"


def resolver_data_inicio(config_geral):
    """
    Retorna a data de início do processamento conforme a configuração.
    - Se carga_inicial=true, usa a data_de_inicio configurada.
    - Caso contrário, usa os últimos 7 dias.
    """
    usar_carga = config_geral.get('carga_inicial', 'false').lower() == 'true'

    if usar_carga:
        data_inicio_str = config_geral.get('data_de_inicio', '')
        try:
            data_inicio = datetime.strptime(data_inicio_str, '%d/%m/%Y').date()
            logging.info(f"🚀 MODO CARGA: Respeitando data fixa: {data_inicio}")
            return data_inicio
        except Exception:
            pass

    data_inicio = (datetime.now() - timedelta(days=7)).date()
    logging.info(f"📅 MODO NORMAL: Processando apenas últimos 7 dias (desde {data_inicio})")
    return data_inicio


def identificar_tipo_evento(lancamen_str):
    """
    Retorna (is_devolucao, is_venda, tipo_evento_log) para um código de lançamento.
    """
    is_devolucao = lancamen_str in CODIGOS_DEVOLUCAO
    is_venda = lancamen_str in CODIGOS_VENDA
    tipo_evento_log = lancamen_str if is_devolucao else 'VENDA'
    return is_devolucao, is_venda, tipo_evento_log