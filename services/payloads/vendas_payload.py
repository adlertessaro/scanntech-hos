# scanntech/services/payloads/vendas_payload.py

from scanntech.db.conexao import conectar
from datetime import datetime, timezone, timedelta
import unicodedata
import json

# (Suas constantes e funções auxiliares permanecem inalteradas)
CANAIS_VENDA = {1: "VENDA NA LOJA", 2: "E-COMMERCE", 3: "IFOOD", 4: "RAPPI", 5: "WHATSAPP", 6: "OUTROS", 7: "ZÉ DELIVERY"}
FINALIZADORAS = {9: "DINHEIRO", 10: "CARTÃO CREDITO", 11: "CHEQUE", 12: "OUTROS", 13: "CARTÃO DEBITO", 14: "PIX", 15: "VALE"}

def limpar_codigo_barras(cod_barra):
    if cod_barra is None: return ""
    if isinstance(cod_barra, float): return str(int(cod_barra))
    elif isinstance(cod_barra, str) and cod_barra.endswith('.0'): return cod_barra[:-2]
    return str(cod_barra)

def remove_acentos(texto):
    if not texto: return "Nao Informado"
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn')


def montar_payload_da_venda(venda, empresa, config, nrcaixa, is_devolucao=False, cupom=None, lancamen=None, valor_total=None, force_as_sale=False):
    # O tipo de transação para os logs iniciais pode ser ajustado
    tipo_log = 'VENDA (reconstruída)' if force_as_sale else 'CANCELAMENTO/DEVOLUÇÃO' if is_devolucao else 'VENDA'
    print(f"➡️ Empresa {empresa} | Caixa {nrcaixa}")
    print(f"   • Montando lote da {tipo_log} referente à venda {venda}")

    venda = int(venda)
    empresa = int(empresa)

    conn = conectar()
    cur = conn.cursor()

    try:
        # A lógica de busca de dados gerais e pagamentos permanece a mesma
        cur.execute(
            "SELECT data, hora, cupom, valor, dinheiro, cheque, outros FROM caixa WHERE venda = %s AND empresa = %s",
            (venda, empresa)
        )
        caixa_row = cur.fetchone()
        if not caixa_row:
            raise ValueError(f"Transação {venda} não encontrada na tabela 'caixa'.")
        # (Lógica de data, pagamentos, etc. continua aqui...)
        data, hora, cupom_db, valor_total_db, dinheiro, cheque, outros = caixa_row
        cupom = cupom or cupom_db
        valor_total = valor_total or valor_total_db
        dt = datetime.combine(data, hora or datetime.strptime("00:00:00", "%H:%M:%S").time())
        dt = dt.replace(tzinfo=timezone(timedelta(hours=-3)))
        dt_iso = dt.strftime("%Y-%m-%dT%H:%M:%S.000%z")
        pagamentos = []
        finalizadoras_desc = []
        try:
            # ALTERADO: Removido "DISTINCT ON (v.cartao)" para buscar todos os pagamentos de cartão, inclusive os repetidos.
            cur.execute("""
                SELECT v.valor, fc.tipo 
                FROM vendastef v 
                JOIN fin_cartoes fc ON v.cartao = fc.descricao 
                WHERE v.nvenda = %s AND v.empresa = %s
            """, (venda, empresa))
            for valor_cartao, tipo_cartao in cur.fetchall():
                tipo_cartao_fmt = (tipo_cartao or "").strip().upper()
                codigo_pgto = 10 if tipo_cartao_fmt == "CREDITO" else 13 if tipo_cartao_fmt == "DEBITO" else 12
                pagamentos.append({"codigoTipoPago": codigo_pgto, "importe": round(float(valor_cartao), 2)})
                finalizadoras_desc.append(FINALIZADORAS.get(codigo_pgto, "OUTROS"))
        except Exception as e:
            print(f"⚠️ Aviso: Erro ao buscar pagamentos de cartão para a venda {venda}: {e}")
        if float(dinheiro or 0) > 0:
            pagamentos.append({"codigoTipoPago": 9, "importe": round(float(dinheiro), 2)})
            finalizadoras_desc.append(FINALIZADORAS[9])
        if float(cheque or 0) > 0:
            pagamentos.append({"codigoTipoPago": 11, "importe": round(float(cheque), 2)})
            finalizadoras_desc.append(FINALIZADORAS[11])
        if float(outros or 0) > 0:
            pagamentos.append({"codigoTipoPago": 12, "importe": round(float(outros), 2)})
            finalizadoras_desc.append(FINALIZADORAS[12])
        if not pagamentos and valor_total and float(valor_total) > 0:
            pagamentos.append({"codigoTipoPago": 9, "importe": round(float(valor_total), 2)})
            finalizadoras_desc.append(FINALIZADORAS[9])
        for pgto in pagamentos:
            pgto.update({"codigoMoneda": "986", "cotizacion": 1.00})
        canal_codigo = 1
        try:
            cur.execute("SELECT modo_captacao FROM pedidos_farma WHERE venda = %s AND empresa = %s", (venda, empresa))
            row = cur.fetchone()
            if row and row[0]:
                modo = row[0].strip().upper()
                if modo == "IFOOD": canal_codigo = 3
                elif modo == "E-COMMERCE": canal_codigo = 2
                elif modo == "WHATSAPP": canal_codigo = 5
        except Exception as e:
            print(f"⚠️ Aviso: Erro ao obter canal de venda para {venda}: {e}")
        canal_descricao = CANAIS_VENDA.get(canal_codigo, "OUTROS")
        
        detalhes = []
        desconto_total = 0.0
        recargo_total = 0.0 
        cancelada_flag = is_devolucao

        if is_devolucao:
            # Esta seção agora serve tanto para cancelamentos REAIS quanto para reconstruir VENDAS,
            # pois a fonte de dados ('itens_cancelados') é a mesma.
            if force_as_sale:
                print("   • MODO RECONSTRUÇÃO: Usando 'itens_cancelados' para montar uma VENDA.")
            
            cur.execute("""
                SELECT ic.produto, cb.cod_barra, p.descricao, ic.quantidade, ic.valor, ic.desconto
                FROM itens_cancelados ic
                LEFT JOIN produtos p ON ic.produto = p.codigo
                LEFT JOIN cod_barras cb ON ic.produto = cb.codigo
                WHERE ic.n_venda = %s AND ic.empresa = %s
            """, (venda, empresa))
            itens_cancelados = cur.fetchall()

            if not itens_cancelados:
                # Se não houver itens cancelados, não há como reconstruir a venda ou o cancelamento.
                raise ValueError(f"Transação {venda} marcada como devolução/cancelamento não possui itens em 'itens_cancelados'.")

            for item_row in itens_cancelados:
                # ... (a lógica de processamento dos itens permanece exatamente a mesma) ...
                (prod, cod_barra, desc, qtd, valor_unitario, desconto_db) = item_row
                qtd_val = float(qtd or 0.0)
                valor_unitario_val = float(valor_unitario or 0.0)
                desconto_item = float(desconto_db or 0.0)
                importe_linha_payload = qtd_val * valor_unitario_val
                desconto_total += desconto_item
                detalhes.append({
                    "codigoArticulo": str(int(prod)),
                    "codigoBarras": limpar_codigo_barras(cod_barra),
                    "descripcionArticulo": remove_acentos(desc or "Nao Informado"),
                    "cantidad": qtd_val,
                    "importeUnitario": round(valor_unitario_val, 2),
                    "importe": round(importe_linha_payload, 2),
                    "descuento": round(desconto_item, 2),
                    "recargo": 0.0
                })
        
        # O bloco 'if not detalhes' agora só deve ser executado para VENDAS normais
        if not detalhes: 
            print("   • Tratando como Venda padrão. Buscando em 'vendidos'.")
            cur.execute("""
                SELECT v.produto, v.descricao, v.quanti, v.preco, v.precovenda, v.devolvido, 
                       p.cod_barra, v.acrescimo, v.precovenda_cadastro
                FROM vendidos v LEFT JOIN produtos p ON p.codigo = v.produto 
                WHERE v.venda = %s AND v.empresa = %s
            """, (venda, empresa))

            itens = cur.fetchall()
            if not itens: raise ValueError(f"Venda {venda} não possui itens em 'vendidos'.")
            
            soma_quanti = sum(float(i[2]) for i in itens)
            soma_devolvido = sum(float(i[5] or 0) for i in itens)
            if soma_quanti > 0 and soma_quanti == soma_devolvido:
                cancelada_flag = True
            
            for item_row in itens:
                (prod, desc, qtd, preco_db, precovenda_db, dev, 
                 cod_barra, acrescimo_db, precovenda_cadastro_db) = item_row

                desconto_item = 0.0
                recargo_item = 0.0
                importe_unitario_payload = 0.0
                importe_linha_payload = 0.0
                
                qtd_val = float(qtd or 0.0)
                acrescimo = (acrescimo_db or "").strip().upper()

                if acrescimo == 'SIM':
                    preco_base = float(preco_db or 0.0)
                    preco_final = float(precovenda_cadastro_db or 0.0)
                    importe_unitario_payload = preco_final
                    recargo_item = abs(preco_final - preco_base) * qtd_val
                    importe_linha_payload = (preco_final * qtd_val) + recargo_item
                else:
                    preco_bruto = float(preco_db or 0.0)
                    preco_liquido = float(precovenda_db or 0.0)
                    importe_unitario_payload = preco_bruto
                    diferenca = preco_bruto - preco_liquido
                    if diferenca > 0:
                        desconto_item = diferenca * qtd_val
                    importe_linha_payload = preco_liquido * qtd_val

                desconto_total += desconto_item
                recargo_total += recargo_item

                detalhes.append({
                    "codigoArticulo": str(int(prod)),
                    "codigoBarras": limpar_codigo_barras(cod_barra),
                    "descripcionArticulo": remove_acentos(desc or "Nao Informado"),
                    "cantidad": qtd_val,
                    "importeUnitario": round(importe_unitario_payload, 2),
                    "importe": round(importe_linha_payload, 2),
                    "descuento": round(desconto_item, 2),
                    "recargo": round(recargo_item, 2)
                })

        if force_as_sale:
            cancelada_flag = False

        numero_base = abs(int(float(cupom)))
        
        # ALTERADO: Removido "sorted(list(set(...)))" para manter as finalizadoras duplicadas e a ordem.
        descricao_finalizadoras = '-'.join(finalizadoras_desc)
        
        payload = {
            "fecha": dt_iso,
            "numero": f"-{str(numero_base).zfill(8)}" if cancelada_flag else str(numero_base).zfill(8),
            "total": round(float(valor_total), 2),
            "codigoMoneda": "986",
            "cotizacion": 1.00,
            "descuentoTotal": round(desconto_total, 2),
            "recargoTotal": round(recargo_total, 2),
            "cancelacion": cancelada_flag,
            "codigoCanalVenta": canal_codigo,
            "descripcionCanalVenta": f"{canal_descricao}-{descricao_finalizadoras}",
            "detalles": detalhes,
            "pagos": pagamentos
        }

        print(f"   • Payload final montado para {tipo_log} {venda}:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return payload

    except (ValueError, Exception) as e:
        print(f"❌ Erro crítico ao montar payload para venda {venda}: {e}")
        raise

    finally:
        if conn and not conn.closed:
            if cur: cur.close()
            conn.close()
            print(f"   • Conexão com o banco de dados fechada para transação da venda {venda}.")