# scanntech/services/processors/vendas_lote_builder.py
"""
Responsável por iterar as vendas brutas da fila, aplicar todas as validações
e construir os dicionários 'payloads' e 'vendas_enviadas' prontos para o envio.
"""

import logging

from scanntech.services.processors.vendas_db_helpers import (
    excluir_venda_da_fila,
    verificar_venda_ja_processada,
    verificar_duplicata_por_cupom,
)
from scanntech.services.processors.vendas_utils import (
    CODIGOS_ACEITOS,
    identificar_tipo_evento,
)
from scanntech.services.payloads.vendas_payload import montar_payload_da_venda


def construir_lote(cur, conn, vendas, empresa_erp, estacao_original, estacao_limitada, config_completa_loja, data_inicio):
    """
    Para cada venda da lista, aplica as validações e monta o payload.

    Retorna:
        payloads (list): lista de dicts prontos para enviar à API.
        vendas_enviadas (dict): mapa venda_id -> {tipo_evento, data_venda, valor_total, payload}.
    """
    payloads = []
    vendas_enviadas = {}

    for venda in vendas:
        try:
            cur.execute(
                "SELECT lancamen, cupom, valor, data, operador FROM caixa WHERE venda = %s AND empresa = %s",
                (venda, empresa_erp)
            )
            row = cur.fetchone()

            # ── Sem registro no caixa: remove da fila ──────────────────────────
            if not row:
                excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                conn.commit()
                continue

            lancamen, cupom, valor, data_venda, operador = row
            lancamen_str = str(lancamen or '').strip().upper()

            # ── Lançamento administrativo: remove da fila ──────────────────────
            if lancamen_str not in CODIGOS_ACEITOS:
                logging.info(f"⏭️ Removendo lançamento administrativo da fila: {lancamen_str}")
                excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                conn.commit()
                continue

            # ── Venda anterior à data de início: remove da fila ───────────────
            if data_inicio and data_venda < data_inicio:
                logging.info(
                    f"👻 Fantasma detectado/Antigo: Venda {venda} (Data {data_venda}) "
                    f"< Início ({data_inicio}). Removendo da fila."
                )
                excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                conn.commit()
                continue

            is_devolucao, is_venda, tipo_evento_log = identificar_tipo_evento(lancamen_str)

            # ── Validação 1: ID de venda já processado ─────────────────────────
            if verificar_venda_ja_processada(cur, venda, empresa_erp, tipo_evento_log):
                logging.info(
                    f"⏭️ Venda ID {venda} (Cupom {cupom}) já consta nos logs de sucesso. "
                    f"Removendo da fila."
                )
                excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                conn.commit()
                continue

            # ── Validação 2: duplicata por cupom/valor/lancamen/operador/estacao
            if verificar_duplicata_por_cupom(
                cur, venda, empresa_erp, cupom, valor,
                lancamen_str, operador, estacao_original
            ):
                excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                conn.commit()
                continue

            # ── Montar payload ─────────────────────────────────────────────────
            payload = montar_payload_da_venda(
                venda,
                empresa_erp,
                config_completa_loja,
                estacao_limitada,
                is_devolucao=is_devolucao,
                cupom=cupom,
                valor_total=valor,
                debug_mode=False
            )

            if payload:
                if is_devolucao:
                    logging.info(f"    🔄 Venda {venda} enviada como CANCELAMENTO (Tipo: {tipo_evento_log})")
                else:
                    logging.info(f"    🛒 Venda {venda} enviada como VENDA (Tipo: {tipo_evento_log})")

                payloads.append(payload)
                vendas_enviadas[venda] = {
                    'tipo_evento': tipo_evento_log,
                    'data_venda': data_venda,
                    'valor_total': valor,
                    'payload': payload,
                }

        except Exception as erro_individual:
            print(f"❌ Erro ao processar venda {venda}: {erro_individual}")
            conn.rollback()
            cur.execute("""
                UPDATE int_scanntech_vendas
                SET tentativas = tentativas + 1, erro = %s, data_hora_tentativa = NOW()
                WHERE venda = %s AND empresa = %s AND estacao = %s
            """, (str(erro_individual)[:255], venda, empresa_erp, estacao_original))
            conn.commit()

    return payloads, vendas_enviadas