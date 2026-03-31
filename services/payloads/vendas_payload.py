# scanntech/services/payloads/vendas_payload.py
"""
Ponto de entrada para a montagem do payload de vendas enviado à Scanntech.

Responsabilidades deste arquivo:
  - Buscar os dados principais da tabela caixa
  - Montar o timestamp ISO
  - Delegar pagamentos e detalhes para submódulos
  - Montar e retornar o dict final do payload

Submódulos:
  vendas_payload_helpers.py    → constantes e funções puras
  vendas_payload_pagamentos.py → construção da lista 'pagos'
  vendas_payload_detalhes.py   → construção da lista 'detalles'
"""

import json
import logging
from datetime import datetime, timezone, timedelta

from scanntech.db.conexao import conectar
from scanntech.services.payloads.vendas_payload_helpers import (
    CANAIS_VENDA,
    converter_para_float,
)
from scanntech.services.payloads.vendas_payload_pagamentos import construir_pagamentos
from scanntech.services.payloads.vendas_payload_detalhes import construir_detalhes


def buscar_venda_original(venda_dev, empresa, cupom):
    """Localiza o ID da venda original (VV) para um cupom que está sendo cancelado."""
    codigos_aceitos = ['VV', 'VP', 'VC', 'CR', 'CH', 'CP']
    conn = conectar()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT venda FROM caixa
            WHERE cupom = %s AND empresa = %s AND lancamen = ANY(%s)
              AND venda < %s
            ORDER BY venda DESC LIMIT 1
        """, (cupom, empresa, codigos_aceitos, venda_dev))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()
        conn.close()


def montar_payload_da_venda(
    venda, empresa, config, estacao,
    is_devolucao=False, cupom=None, lancamen=None,
    valor_total=None, force_as_sale=False, debug_mode=False
):
    log_avancado = config.get('log_avancado', 'false').lower() == 'true'

    venda = int(venda)
    empresa = int(empresa)
    cancelada_flag = is_devolucao

    conn = conectar()
    cur = conn.cursor()

    try:
        # ── Dados principais do caixa ──────────────────────────────────────────
        cur.execute("""
            SELECT data, hora, cupom, valor, dinheiro, cheque, outros, cartao,
                   aprazo, convenio, pgto_credito, pgto_fidelidade,
                   valor_deposito, valor_boleto, valor_transferencia,
                   valor_pecfebrafar, valor_boleto_parcelado, valor_cartao_presente
            FROM caixa
            WHERE venda = %s AND empresa = %s
        """, (venda, empresa))

        caixa_row = cur.fetchone()
        if not caixa_row:
            raise ValueError(f"Transação venda: {venda} não encontrada na tabela 'caixa'.")

        (data_db, hora, cupom_db, valor_total_db, *_) = caixa_row

        cupom = cupom or cupom_db
        valor_total = abs(float(valor_total or valor_total_db))
        data_para_envio = data_db

        if is_devolucao and log_avancado:
            logging.info(
                f"      🕒 CANCELAMENTO: Mantendo data do lançamento atual "
                f"para o fechamento: {data_para_envio}"
            )

        # ── Log avançado: início ───────────────────────────────────────────────
        if log_avancado:
            logging.info(f"\n{'='*80}")
            logging.info(f"🔍 LOG AVANÇADO - Venda {venda} | Empresa {empresa} | Estação {estacao}")
            logging.info(f"{'='*80}")
            logging.info(f"   📅 Data Real: {data_db} | Data Envio: {data_para_envio} | Hora: {hora}")
            logging.info(f"   💵 Valor Total Caixa: R$ {valor_total}")
            logging.info(f"   🧾 Cupom: {cupom}")
            logging.info(f"   🏷️  Tipo: {'DEVOLUÇÃO/CANCELAMENTO' if is_devolucao else 'VENDA'}")

        # ── Timestamp ISO ──────────────────────────────────────────────────────
        dt = datetime.combine(
            data_para_envio,
            hora or datetime.strptime("00:00:00", "%H:%M:%S").time()
        )
        dt = dt.replace(tzinfo=timezone(timedelta(hours=-3)))
        dt_iso = dt.strftime("%Y-%m-%dT%H:%M:%S.000%z")

        valor_total_cupom = round(converter_para_float(valor_total), 2)

        # ── Pagamentos ─────────────────────────────────────────────────────────
        pagamentos, finalizadoras_desc = construir_pagamentos(
            cur, venda, empresa, caixa_row, valor_total_cupom, log_avancado
        )

        # ── Canal de venda ─────────────────────────────────────────────────────
        canal_codigo = _resolver_canal_venda(cur, venda, empresa)
        canal_descricao = CANAIS_VENDA.get(canal_codigo, "OUTROS")

        # ── Detalhes (itens) ───────────────────────────────────────────────────
        detalhes = construir_detalhes(
            cur, venda, empresa, is_devolucao, force_as_sale,
            data_db, valor_total_cupom, log_avancado
        )

        # ── Montar payload final ───────────────────────────────────────────────
        numero_base = abs(int(float(cupom)))
        descricao_finalizadoras = '-'.join(finalizadoras_desc)

        total_final = round(sum(d['importe'] for d in detalhes), 2)
        desconto_total = sum(d['descuento'] for d in detalhes)
        recargo_total = sum(d['recargo'] for d in detalhes)

        payload = {
            "fecha": dt_iso,
            "numero": f"-{str(numero_base).zfill(8)}" if cancelada_flag else str(numero_base).zfill(8),
            "total": total_final,
            "codigoMoneda": "986",
            "cotizacion": 1.00,
            "descuentoTotal": round(desconto_total, 2),
            "recargoTotal": round(recargo_total, 2),
            "cancelacion": cancelada_flag,
            "codigoCanalVenta": canal_codigo,
            "descripcionCanalVenta": f"{canal_descricao}-{descricao_finalizadoras}",
            "detalles": detalhes,
            "pagos": pagamentos,
        }

        if log_avancado:
            logging.info(f"\n   🎯 PAYLOAD FINAL:")
            logging.info(json.dumps(payload, indent=2, ensure_ascii=False))
            logging.info(f"\n   ✅ VALIDAÇÃO FINAL:")
            logging.info(f"      Total do cupom (enviado): R$ {total_final:.2f}")
            logging.info(f"      Desconto Total: R$ {desconto_total:.2f}")
            logging.info(f"      Recargo Total: R$ {recargo_total:.2f}")
            logging.info(f"{'='*80}\n")

        return payload

    except (ValueError, Exception) as e:
        logging.error(f"❌ Erro crítico ao montar payload para venda {venda}: {e}")
        raise
    finally:
        if conn and not conn.closed:
            if cur:
                cur.close()
            conn.close()


# ──────────────────────────────────────────────────────────────────────────────

def _resolver_canal_venda(cur, venda, empresa):
    try:
        cur.execute(
            "SELECT modo_captacao FROM pedidos_farma WHERE venda = %s AND empresa = %s",
            (venda, empresa)
        )
        row = cur.fetchone()
        if row and row[0]:
            modo = row[0].strip().upper()
            return {'IFOOD': 3, 'ECOMMERCE': 2, 'WHATSAPP': 5, 'PADRAO': 1}.get(modo, 6)
    except Exception:
        pass
    return 1