import logging
from services.payloads.vendas_payload_helpers import (
    FINALIZADORAS,
    converter_para_float,
)


def construir_pagamentos(cur, venda, empresa, caixa_row, valor_total_cupom, log_avancado):
    """
    Monta a lista de pagamentos combinando cartões (vendastef),
    dinheiro, cheque, outros e finalizadoras diversas do caixa.

    Retorna:
        pagamentos (list): lista de dicts com codigoTipoPago, importe, etc.
        finalizadoras_desc (list): lista de strings para montar descripcionCanalVenta.
    """
    (_, _, _, _, dinheiro, cheque, outros, _,
     aprazo, convenio, pgto_credito, pgto_fidelidade,
     valor_deposito, valor_boleto, valor_transferencia,
     valor_pecfebrafar, valor_boleto_parcelado, valor_cartao_presente) = caixa_row

    pagamentos = []
    finalizadoras_desc = []
    soma_pagamentos_calculada = 0.0

    if log_avancado:
        logging.info(f"\n   💳 CONSTRUINDO PAGAMENTOS:")

    # 1. Cartões (vendastef)
    pagamentos, finalizadoras_desc, soma_pagamentos_calculada = _adicionar_cartoes(
        cur, venda, empresa, pagamentos, finalizadoras_desc,
        soma_pagamentos_calculada, log_avancado
    )

    # 2. Dinheiro
    dinheiro_val = converter_para_float(dinheiro)
    if dinheiro_val > 0:
        dinheiro_val = round(dinheiro_val, 2)
        pagamentos.append({"codigoTipoPago": 9, "importe": dinheiro_val})
        finalizadoras_desc.append(FINALIZADORAS[9])
        soma_pagamentos_calculada += dinheiro_val
        if log_avancado:
            logging.info(f"      ✓ Dinheiro: R$ {dinheiro_val}")

    # 3. Cheque
    cheque_val = converter_para_float(cheque)
    if cheque_val > 0:
        cheque_val = round(cheque_val, 2)
        pagamentos.append({"codigoTipoPago": 11, "importe": cheque_val})
        finalizadoras_desc.append(FINALIZADORAS[11])
        soma_pagamentos_calculada += cheque_val
        if log_avancado:
            logging.info(f"      ✓ Cheque: R$ {cheque_val}")

    # 4. Outros
    outros_val = converter_para_float(outros)
    if outros_val > 0:
        outros_val = round(outros_val, 2)
        pagamentos.append({"codigoTipoPago": 12, "importe": outros_val})
        finalizadoras_desc.append(FINALIZADORAS[12])
        soma_pagamentos_calculada += outros_val
        if log_avancado:
            logging.info(f"      ✓ Outros: R$ {outros_val}")

    # 5. Finalizadoras diversas
    finalizadoras_valor = sum(converter_para_float(v) for v in [
        aprazo, convenio, pgto_credito, pgto_fidelidade,
        valor_deposito, valor_boleto, valor_transferencia,
        valor_pecfebrafar, valor_boleto_parcelado, valor_cartao_presente,
    ])
    if finalizadoras_valor > 0:
        finalizadoras_valor = round(finalizadoras_valor, 2)
        pagamentos.append({"codigoTipoPago": 15, "importe": finalizadoras_valor})
        finalizadoras_desc.append(FINALIZADORAS[15])
        soma_pagamentos_calculada += finalizadoras_valor
        if log_avancado:
            logging.info(f"      ✓ Finalizadora: R$ {finalizadoras_valor}")

    # 6. Fallback: sem nenhum pagamento encontrado
    if not pagamentos and valor_total_cupom > 0:
        pagamentos.append({"codigoTipoPago": 9, "importe": round(valor_total_cupom, 2)})
        finalizadoras_desc.append(FINALIZADORAS[9])
        soma_pagamentos_calculada += round(valor_total_cupom, 2)
        if log_avancado:
            logging.info(f"      ✓ Dinheiro (fallback): R$ {round(valor_total_cupom, 2)}")

    # Ajuste de diferença de arredondamento
    diferenca = round(soma_pagamentos_calculada - valor_total_cupom, 2)
    if log_avancado:
        logging.info(f"\n   💰 VALIDAÇÃO DE PAGAMENTOS:")
        logging.info(f"      Soma Pagamentos: R$ {soma_pagamentos_calculada:.2f}")
        logging.info(f"      Total Cupom: R$ {valor_total_cupom:.2f}")
        logging.info(f"      Diferença: R$ {diferenca:.2f}")

    if abs(diferenca) > 0.01 and pagamentos:
        pagamentos[-1]["importe"] = round(pagamentos[-1]["importe"] - diferenca, 2)
        if log_avancado:
            logging.info(f"      ✅ Ajustado último pagamento para: R$ {pagamentos[-1]['importe']:.2f}")

    # Adicionar moeda e cotação
    for pgto in pagamentos:
        pgto.update({"codigoMoneda": "986", "cotizacion": 1.00})

    return pagamentos, finalizadoras_desc


# ──────────────────────────────────────────────────────────────────────────────

def _adicionar_cartoes(cur, venda, empresa, pagamentos, finalizadoras_desc, soma, log_avancado):
    try:
        cur.execute("""
            SELECT v.valor, fc.tipo, v.rede
            FROM vendastef v
            JOIN fin_cartoes fc ON v.cartao = fc.descricao
            WHERE v.nvenda = %s AND v.empresa = %s
        """, (venda, empresa))

        cartoes_encontrados = {}

        for valor_cartao, tipo_cartao, rede in cur.fetchall():
            tipo_cartao_fmt = (tipo_cartao or "").strip().upper()
            rede_fmt = (rede or "").strip().upper()

            if rede_fmt == "PIX":
                codigo_pgto = 14
                pagamento_pix = {
                    "codigoTipoPago": 14,
                    "importe": round(float(valor_cartao), 2),
                    "codigoProveedorQR": 1,
                    "codigoBanco": None,
                    "descripcionBanco": None
                }
                key_pix = f"{codigo_pgto}_{pagamento_pix['importe']}"
                if key_pix in cartoes_encontrados:
                    if log_avancado:
                        logging.info(
                            f"      ⚠️  DUPLICATA: PIX R$ {pagamento_pix['importe']} (PULANDO)"
                        )
                    continue
                cartoes_encontrados[key_pix] = True
                pagamentos.append(pagamento_pix)
                finalizadoras_desc.append(FINALIZADORAS.get(codigo_pgto, "PIX"))
                soma += pagamento_pix['importe']
                if log_avancado:
                    logging.info(f"      ✓ PIX: R$ {pagamento_pix['importe']}")
                continue
            elif tipo_cartao_fmt == "CREDITO":
                codigo_pgto = 10
            elif tipo_cartao_fmt == "DEBITO":
                codigo_pgto = 13
            else:
                codigo_pgto = 12

            valor_arredondado = round(float(valor_cartao), 2)
            chave = f"{codigo_pgto}_{valor_arredondado}"

            if chave in cartoes_encontrados:
                if log_avancado:
                    logging.info(
                        f"      ⚠️  DUPLICATA: {FINALIZADORAS.get(codigo_pgto)} "
                        f"R$ {valor_arredondado} (PULANDO)"
                    )
                continue

            cartoes_encontrados[chave] = True
            pagamentos.append({"codigoTipoPago": codigo_pgto, "importe": valor_arredondado})
            finalizadoras_desc.append(FINALIZADORAS.get(codigo_pgto, "OUTROS"))
            soma += valor_arredondado

            if log_avancado:
                logging.info(f"      ✓ {FINALIZADORAS.get(codigo_pgto)}: R$ {valor_arredondado}")

    except Exception as e:
        logging.info(f"⚠️ Aviso: Erro ao buscar cartões: {e}")

    return pagamentos, finalizadoras_desc, soma