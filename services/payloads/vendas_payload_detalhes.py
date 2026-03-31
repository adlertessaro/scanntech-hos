# scanntech/services/payloads/vendas_payload_detalhes.py
"""
Responsável por construir a lista de detalhes (itens) do payload de vendas,
incluindo toda a lógica de cancelamentos, devoluções e ajustes de diferença.
"""

import logging
from scanntech.services.payloads.vendas_payload_helpers import (
    limpar_codigo_barras,
    remove_acentos,
)


def construir_detalhes(cur, venda, empresa, is_devolucao, force_as_sale,
                       data_db, valor_total_cupom, log_avancado):
    """
    Retorna a lista de detalhes (itens) do payload após consultas ao banco e
    aplicação de todos os ajustes de diferença.
    """
    if log_avancado:
        logging.info(f"\n   📦 CONSTRUINDO ITENS:")

    if is_devolucao:
        detalhes = _itens_cancelamento(cur, venda, empresa, force_as_sale, data_db, valor_total_cupom, log_avancado)
    else:
        detalhes = _itens_venda_normal(cur, venda, empresa, log_avancado)

    detalhes = _ajustar_diferencas(detalhes, valor_total_cupom, log_avancado)
    detalhes = _filtrar_itens_invalidos(detalhes, venda, log_avancado)

    if not detalhes:
        raise ValueError(f"Venda {venda}: Todos os itens foram removidos. Não é possível enviar venda sem itens.")

    return detalhes


# ──────────────────────────────────────────────────────────────────────────────
# Busca de itens
# ──────────────────────────────────────────────────────────────────────────────

def _itens_venda_normal(cur, venda, empresa, log_avancado):
    if log_avancado:
        logging.info(f"      ✓ Modo: VENDA NORMAL")

    cur.execute("""
        SELECT v.produto, v.descricao, v.quanti, v.preco, v.precovenda, v.devolvido,
               p.cod_barra, v.acrescimo, v.precovenda_cadastro
        FROM vendidos v LEFT JOIN produtos p ON p.codigo = v.produto
        WHERE v.venda = %s AND v.empresa = %s
    """, (venda, empresa))
    itens = cur.fetchall()

    if not itens:
        raise ValueError(f"Venda {venda} não possui itens em 'vendidos'.")

    detalhes = []
    for (prod, desc, qtd, precodb, precovendadb, dev,
         cod_barra, acrescimo_db, precovenda_cadastro_db) in itens:

        qtd_val = float(qtd or 0.0)
        preco_base = float(precodb or 0.0)
        preco_final = float(precovendadb or 0.0)

        acrescimo = (acrescimo_db or "").strip().upper()
        if preco_final == 0 and acrescimo == 'SIM':
            preco_final = float(precovenda_cadastro_db or 0.0)

        importe_unitario_payload = preco_base
        val_base_total = preco_base * qtd_val
        val_final_total = preco_final * qtd_val
        diff_total = val_base_total - val_final_total

        desconto_item = max(diff_total, 0.0) if diff_total > 0.005 else 0.0
        recargo_item = abs(diff_total) if diff_total < -0.005 else 0.0
        importe_linha_payload = val_base_total - desconto_item + recargo_item

        detalhes.append({
            "codigoArticulo": str(int(prod)),
            "codigoBarras": limpar_codigo_barras(cod_barra),
            "descripcionArticulo": remove_acentos(desc or "Nao Informado"),
            "cantidad": qtd_val,
            "importeUnitario": round(importe_unitario_payload, 2),
            "importe": round(importe_linha_payload, 2),
            "descuento": round(desconto_item, 2),
            "recargo": round(recargo_item, 2),
        })

    return detalhes


def _itens_cancelamento(cur, venda, empresa, force_as_sale, data_db, valor_total_cupom, log_avancado):
    if force_as_sale:
        if log_avancado:
            logging.info(f"      🔄 Modo: RECONSTRUÇÃO (busca cancelados, envia como VENDA)")
        itens = _buscar_itens_cancelados_sem_data(cur, venda, empresa)
        if not itens:
            itens = _buscar_devolvidos(cur, venda, empresa)
        if not itens:
            raise ValueError(f"Venda {venda} não possui itens em 'itens_cancelados' nem 'devolvidos'.")
    else:
        if log_avancado:
            logging.info(f"      ❌ Modo: CANCELAMENTO REAL (Filtrando por Data/Hora)")
        itens = _buscar_itens_cancelados_com_data(cur, venda, empresa, data_db, valor_total_cupom, log_avancado)
        if not itens:
            itens = _buscar_devolvidos(cur, venda, empresa)
        if not itens:
            raise ValueError(f"Cancelamento {venda} não possui itens válidos.")

    return _processar_itens_cancelamento(itens)


def _processar_itens_cancelamento(itens):
    detalhes = []
    for (prod, cod_barra, desc, qtd, valor_liquido_db,
         desconto_percentual_db, preco_cadastro_db) in itens:

        qtd_val = float(qtd or 0.0)
        preco_liquido = float(valor_liquido_db or 0.0)
        preco_bruto = float(preco_cadastro_db or 0.0) or preco_liquido

        val_bruto_total = preco_bruto * qtd_val
        val_liquido_total = preco_liquido * qtd_val
        diff_total = val_bruto_total - val_liquido_total

        desconto_item = diff_total if diff_total > 0.005 else 0.0
        recargo_item = abs(diff_total) if diff_total < -0.005 else 0.0
        importe_linha_payload = val_liquido_total

        detalhes.append({
            "codigoArticulo": str(int(prod)),
            "codigoBarras": limpar_codigo_barras(cod_barra),
            "descripcionArticulo": remove_acentos(desc or "Nao Informado"),
            "cantidad": qtd_val,
            "importeUnitario": round(preco_bruto, 2),
            "importe": round(importe_linha_payload, 2),
            "descuento": round(desconto_item, 2),
            "recargo": round(recargo_item, 2),
        })

    return detalhes


# ──────────────────────────────────────────────────────────────────────────────
# Queries de busca de itens
# ──────────────────────────────────────────────────────────────────────────────

def _buscar_itens_cancelados_sem_data(cur, venda, empresa):
    cur.execute("""
        SELECT ic.produto,
               (SELECT cb.cod_barra FROM cod_barras cb WHERE cb.codigo = ic.produto LIMIT 1),
               p.descricao, ic.quantidade, ic.valor, ic.desconto, p.prc_venda
        FROM itens_cancelados ic
        LEFT JOIN produtos p ON ic.produto = p.codigo
        WHERE ic.n_venda = %s AND ic.empresa = %s
    """, (venda, empresa))
    return cur.fetchall()


def _buscar_itens_cancelados_com_data(cur, venda, empresa, data_db, valor_total_cupom, log_avancado):
    cur.execute("""
        SELECT ic.produto,
               (SELECT cb.cod_barra FROM cod_barras cb WHERE cb.codigo = ic.produto LIMIT 1),
               p.descricao, ic.quantidade, ic.valor, ic.desconto, p.prc_venda
        FROM itens_cancelados ic
        LEFT JOIN produtos p ON ic.produto = p.codigo
        WHERE ic.n_venda = %s AND ic.empresa = %s AND ic.data = %s
    """, (venda, empresa, data_db))
    itens = cur.fetchall()

    if not itens:
        if log_avancado:
            logging.warning("      ⚠️ Itens não encontrados com a data exata. Tentando busca ampla...")
        cur.execute("""
            SELECT ic.produto,
                   (SELECT cb.cod_barra FROM cod_barras cb WHERE cb.codigo = ic.produto LIMIT 1),
                   p.descricao, ic.quantidade, ic.valor, ic.desconto, p.prc_venda
            FROM itens_cancelados ic
            LEFT JOIN produtos p ON ic.produto = p.codigo
            WHERE ic.n_venda = %s AND ic.empresa = %s
        """, (venda, empresa))
        itens_todos = cur.fetchall()

        # Tenta filtrar apenas os que somam o valor do cupom
        soma_alvo = float(valor_total_cupom or 0)
        for it in itens_todos:
            val_item = float(it[4] or 0) * float(it[3] or 0)
            if abs(val_item - soma_alvo) < 0.05:
                return [it]
        itens = itens_todos

    return itens


def _buscar_devolvidos(cur, venda, empresa):
    cur.execute("""
        SELECT d.produto,
               (SELECT cb.cod_barra FROM cod_barras cb WHERE cb.codigo = d.produto LIMIT 1),
               p.descricao, d.quanti, ROUND(d.preco::numeric, 2), 0, p.prc_venda
        FROM devolvidos d
        LEFT JOIN produtos p ON d.produto = p.codigo
        WHERE d.venda = %s AND d.empresa = %s
    """, (venda, empresa))
    return cur.fetchall()


# ──────────────────────────────────────────────────────────────────────────────
# Ajustes de diferença e filtragem
# ──────────────────────────────────────────────────────────────────────────────

def _ajustar_diferencas(detalhes, valor_total_cupom, log_avancado):
    soma_itens = sum(d['importe'] for d in detalhes)
    diferenca = round(soma_itens - valor_total_cupom, 2)

    if log_avancado:
        logging.info(f"\n   📊 VALIDAÇÃO DE ITENS:")
        logging.info(f"      Soma Itens (importe): R$ {soma_itens:.2f}")
        logging.info(f"      Total Cupom: R$ {valor_total_cupom:.2f}")
        logging.info(f"      Diferença: R$ {diferenca:.2f}")

    if abs(diferenca) <= 0.01 or not detalhes:
        return detalhes

    # Tenta remover item intruso (diferença positiva exatamente igual ao importe de um item)
    if diferenca > 0:
        for i, item in enumerate(detalhes):
            if abs(item['importe'] - diferenca) <= 0.01:
                removido = detalhes.pop(i)
                diferenca = round(diferenca - removido['importe'], 2)
                if log_avancado:
                    logging.info(
                        f"      ✅ CORREÇÃO INTELIGENTE: Item '{removido['descripcionArticulo']}' "
                        f"identificado como sobra e removido. Nova Diferença: R$ {diferenca:.2f}"
                    )
                break

    if abs(diferenca) <= 0.01 or not detalhes:
        return detalhes

    # PBM: desconto maior que o último item → ratear
    if diferenca > 0 and diferenca > detalhes[-1]['importe']:
        if log_avancado:
            logging.warning(f"      ⚠️ PBM Detectado: Desconto (R$ {diferenca}) > Último Item. Rateando...")
        soma_bases = sum(d['importe'] for d in detalhes)
        total_para_ratear = diferenca
        for item in detalhes:
            peso = item['importe'] / soma_bases if soma_bases > 0 else 0
            parte = round(diferenca * peso, 2)
            if item['importe'] - parte < 0:
                parte = item['importe']
            item['importe'] = round(item['importe'] - parte, 2)
            item['descuento'] = round(item['descuento'] + parte, 2)
            total_para_ratear -= parte
        if abs(total_para_ratear) > 0.001:
            detalhes[-1]['importe'] = round(detalhes[-1]['importe'] - total_para_ratear, 2)
            detalhes[-1]['descuento'] = round(detalhes[-1]['descuento'] + total_para_ratear, 2)
    elif diferenca > 0:
        detalhes[-1]['importe'] = round(detalhes[-1]['importe'] - diferenca, 2)
        detalhes[-1]['descuento'] = round(detalhes[-1]['descuento'] + diferenca, 2)
        if log_avancado:
            logging.info(f"      ⚠️ Ajuste fino (Desconto): R$ {diferenca:.2f}")
    else:
        recargo_extra = abs(diferenca)
        detalhes[-1]['importe'] = round(detalhes[-1]['importe'] + recargo_extra, 2)
        detalhes[-1]['recargo'] = round(detalhes[-1]['recargo'] + recargo_extra, 2)
        if log_avancado:
            logging.info(f"      ⚠️ Ajuste fino (Acréscimo): R$ {recargo_extra:.2f}")

    return detalhes


def _filtrar_itens_invalidos(detalhes, venda, log_avancado):
    filtrados = []
    for item in detalhes:
        if item['importe'] < 0.01:
            if log_avancado:
                logging.warning(
                    f"      ⚠️ Item removido (importe = R$ {item['importe']:.2f}): "
                    f"{item['descripcionArticulo']}"
                )
        else:
            filtrados.append(item)
    return filtrados