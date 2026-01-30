from scanntech.db.conexao import conectar
from datetime import datetime, timezone, timedelta
import unicodedata
import json
import logging

CANAIS_VENDA = {1: "VENDA NA LOJA", 2: "E-COMMERCE", 3: "IFOOD", 4: "RAPPI", 5: "WHATSAPP", 6: "OUTROS", 7: "ZÉ DELIVERY"}
FINALIZADORAS = {1: "PIX", 9: "DINHEIRO", 10: "CARTÃO CREDITO", 11: "CHEQUE", 12: "OUTROS", 13: "CARTÃO DEBITO", 15: "FINALIZADORA"}

def limpar_codigo_barras(cod_barra):
    if cod_barra is None: return ""
    if isinstance(cod_barra, float): return str(int(cod_barra))
    elif isinstance(cod_barra, str) and cod_barra.endswith('.0'): return cod_barra[:-2]
    return str(cod_barra)

def remove_acentos(texto):
    if not texto: return "Nao Informado"
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn')

def converter_para_float(valor):
    """Converte valores do banco para float, retornando 0 se inválido."""
    try:
        return float(valor) if valor and valor != 'NENHUM' else 0.0
    except (ValueError, TypeError):
        return 0.0

def montar_payload_da_venda(venda, empresa, config, estacao, is_devolucao=False, cupom=None, lancamen=None, valor_total=None, force_as_sale=False, debug_mode=False):
    # 🔥 VERIFICAR SE LOG AVANÇADO ESTÁ ATIVO NA CONFIG
    log_avancado = config.get('log_avancado', 'false').lower() == 'true'
    
    venda = int(venda)
    empresa = int(empresa)
    
    conn = conectar()
    cur = conn.cursor()
    
    try:
        # ========== BUSCAR DADOS DA TABELA CAIXA ==========
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
        
        (data_db, hora, cupom_db, valor_total_db, dinheiro, cheque, outros, cartao_caixa, 
         aprazo, convenio, pgto_credito, pgto_fidelidade, valor_deposito, 
         valor_boleto, valor_transferencia, valor_pecfebrafar, 
         valor_boleto_parcelado, valor_cartao_presente) = caixa_row
        
        cupom = cupom or cupom_db
        valor_total = abs(float(valor_total or valor_total_db))
        
        # Variável para controlar a data que irá no Payload (inicia com a data do banco)
        data_para_envio = data_db

        # ==============================================================================
        # 🔥 CORREÇÃO: MÁQUINA DO TEMPO (Busca data original)
        # Se for devolução, busca a data da VENDA original para o payload ficar no passado.
        # Mantemos 'data_db' intacta para uso em queries de cancelamento que exigem a data real.
        # ==============================================================================
        if is_devolucao and log_avancado:
            logging.info(f"      🕒 CANCELAMENTO: Mantendo data do lançamento atual para o fechamento: {data_para_envio}")
        # ==============================================================================

        # 🔥 LOG AVANÇADO: Início
        if log_avancado:
            logging.info(f"\n{'='*80}")
            logging.info(f"🔍 LOG AVANÇADO - Venda {venda} | Empresa {empresa} | Estação {estacao}")
            logging.info(f"{'='*80}")
            logging.info(f"   📅 Data Real: {data_db} | Data Envio: {data_para_envio} | Hora: {hora}")
            logging.info(f"   💵 Valor Total Caixa: R$ {valor_total}")
            logging.info(f"   🧾 Cupom: {cupom}")
            logging.info(f"   🏷️  Tipo: {'DEVOLUÇÃO/CANCELAMENTO' if is_devolucao else 'VENDA'}")
        
        # USA data_para_envio PARA GERAR O TIMESTAMP
        dt = datetime.combine(data_para_envio, hora or datetime.strptime("00:00:00", "%H:%M:%S").time())
        dt = dt.replace(tzinfo=timezone(timedelta(hours=-3)))
        dt_iso = dt.strftime("%Y-%m-%dT%H:%M:%S.000%z")
        
        # ========== CONSTRUIR PAGAMENTOS ==========
        pagamentos = []
        finalizadoras_desc = []
        soma_pagamentos_calculada = 0.0
        
        if log_avancado:
            logging.info(f"\n   💳 CONSTRUINDO PAGAMENTOS:")
        
        # 1. Buscar cartões da vendastef
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
                    codigo_pgto = 1
                elif tipo_cartao_fmt == "CREDITO":
                    codigo_pgto = 10
                elif tipo_cartao_fmt == "DEBITO":
                    codigo_pgto = 13
                else:
                    codigo_pgto = 12
                
                valor_cartao_arredondado = round(float(valor_cartao), 2)
                
                # Detectar e pular duplicatas
                chave_cartao = f"{codigo_pgto}_{valor_cartao_arredondado}"
                if chave_cartao in cartoes_encontrados:
                    if log_avancado:
                        logging.info(f"      ⚠️  DUPLICATA: {FINALIZADORAS.get(codigo_pgto)} R$ {valor_cartao_arredondado} (PULANDO)")
                    continue
                
                cartoes_encontrados[chave_cartao] = True
                pagamentos.append({"codigoTipoPago": codigo_pgto, "importe": valor_cartao_arredondado})
                finalizadoras_desc.append(FINALIZADORAS.get(codigo_pgto, "OUTROS"))
                soma_pagamentos_calculada += valor_cartao_arredondado
                
                if log_avancado:
                    logging.info(f"      ✓ {FINALIZADORAS.get(codigo_pgto)}: R$ {valor_cartao_arredondado}")
                    
        except Exception as e:
            logging.info(f"⚠️ Aviso: Erro ao buscar cartões: {e}")
        
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
        
        # 5. Finalizadoras
        finalizadoras_valor = 0
        finalizadoras_valor += converter_para_float(aprazo)
        finalizadoras_valor += converter_para_float(convenio)
        finalizadoras_valor += converter_para_float(pgto_credito)
        finalizadoras_valor += converter_para_float(pgto_fidelidade)
        finalizadoras_valor += converter_para_float(valor_deposito)
        finalizadoras_valor += converter_para_float(valor_boleto)
        finalizadoras_valor += converter_para_float(valor_transferencia)
        finalizadoras_valor += converter_para_float(valor_pecfebrafar)
        finalizadoras_valor += converter_para_float(valor_boleto_parcelado)
        finalizadoras_valor += converter_para_float(valor_cartao_presente)
        
        if finalizadoras_valor > 0:
            finalizadoras_valor = round(finalizadoras_valor, 2)
            pagamentos.append({"codigoTipoPago": 15, "importe": finalizadoras_valor})
            finalizadoras_desc.append(FINALIZADORAS[15])
            soma_pagamentos_calculada += finalizadoras_valor
            if log_avancado:
                logging.info(f"      ✓ Finalizadora: R$ {finalizadoras_valor}")
        
        # 6. Fallback (se não tem pagamento, usa total como dinheiro)
        if not pagamentos and valor_total and converter_para_float(valor_total) > 0:
            valor_total_arredondado = round(converter_para_float(valor_total), 2)
            pagamentos.append({"codigoTipoPago": 9, "importe": valor_total_arredondado})
            finalizadoras_desc.append(FINALIZADORAS[9])
            soma_pagamentos_calculada += valor_total_arredondado
            if log_avancado:
                logging.info(f"      ✓ Dinheiro (fallback): R$ {valor_total_arredondado}")
        
        # Validar e ajustar diferença de pagamentos
        valor_total_cupom = round(converter_para_float(valor_total), 2)
        diferenca_pagamento = round(soma_pagamentos_calculada - valor_total_cupom, 2)
        
        if log_avancado:
            logging.info(f"\n   💰 VALIDAÇÃO DE PAGAMENTOS:")
            logging.info(f"      Soma Pagamentos: R$ {soma_pagamentos_calculada:.2f}")
            logging.info(f"      Total Cupom: R$ {valor_total_cupom:.2f}")
            logging.info(f"      Diferença: R$ {diferenca_pagamento:.2f}")
        
        if abs(diferenca_pagamento) > 0.01 and pagamentos:
            pagamentos[-1]["importe"] = round(pagamentos[-1]["importe"] - diferenca_pagamento, 2)
            if log_avancado:
                logging.info(f"      ✅ Ajustado último pagamento para: R$ {pagamentos[-1]['importe']:.2f}")
        
        # Adicionar moeda e cotação
        for pgto in pagamentos:
            pgto.update({"codigoMoneda": "986", "cotizacion": 1.00})
        
        # ========== CANAL DE VENDA ==========
        canal_codigo = 1
        try:
            cur.execute("SELECT modo_captacao FROM pedidos_farma WHERE venda = %s AND empresa = %s", (venda, empresa))
            row = cur.fetchone()
            if row and row[0]:
                modo = row[0].strip().upper()
                if modo == "IFOOD": canal_codigo = 3
                elif modo == "ECOMMERCE": canal_codigo = 2
                elif modo == "WHATSAPP": canal_codigo = 5
                elif modo == "PADRAO": canal_codigo = 1
                else: canal_codigo = 6
        except Exception as e:
            pass
        
        canal_descricao = CANAIS_VENDA.get(canal_codigo, "OUTROS")
        
        # ========== CONSTRUIR DETALHES (ITENS) ==========
        detalhes = []
        desconto_total = 0.0
        recargo_total = 0.0
        cancelada_flag = False
        
        if log_avancado:
            logging.info(f"\n   📦 CONSTRUINDO ITENS:")
        
        # LÓGICA DE DEVOLUÇÃO/CANCELAMENTO
        if is_devolucao:
            if force_as_sale:
                # Reconstrução: busca em cancelados/devolvidos, monta como VENDA
                if log_avancado:
                    logging.info(f"      🔄 Modo: RECONSTRUÇÃO (busca cancelados, envia como VENDA)")
                
                # QUERY 1: Itens Cancelados (7 colunas)
                cur.execute("""
                    SELECT ic.produto,
                            (SELECT cb.cod_barra FROM cod_barras cb WHERE cb.codigo = ic.produto LIMIT 1) as cod_barra,
                            p.descricao,
                            ic.quantidade,
                            ic.valor,
                            ic.desconto,
                            p.prc_venda  -- 7ª Coluna
                        FROM itens_cancelados ic
                        LEFT JOIN produtos p ON ic.produto = p.codigo
                        WHERE ic.n_venda = %s AND ic.empresa = %s
                """, (venda, empresa))
                itens = cur.fetchall()
                
                if not itens:
                    # QUERY 2: Fallback Devolvidos (7 colunas)
                    cur.execute("""
                        SELECT d.produto,
                               (SELECT cb.cod_barra FROM cod_barras cb WHERE cb.codigo = d.produto LIMIT 1),
                               p.descricao, d.quanti, 
                               ROUND(d.preco::numeric, 2), 
                               0,           -- 6ª Coluna (Desconto dummy)
                               p.prc_venda  -- 7ª Coluna (Preço Cadastro)
                        FROM devolvidos d
                        LEFT JOIN produtos p ON d.produto = p.codigo
                        WHERE d.venda = %s AND d.empresa = %s
                    """, (venda, empresa))
                    itens = cur.fetchall()
                
                if not itens:
                    raise ValueError(f"Venda {venda} não possui itens em 'itens_cancelados' nem 'devolvidos'.")
                
                cancelada_flag = False
                
            else:
                # Cancelamento REAL
                if log_avancado:
                    logging.info(f"      ❌ Modo: CANCELAMENTO REAL (Filtrando por Data/Hora)")
                
                # QUERY 3: Cancelamento Real com Filtro de Data (7 colunas)
                # 🔥 IMPORTANTE: Usamos 'data_db' (data real do cancelamento) para achar o item no banco
                cur.execute("""
                    SELECT ic.produto,
                            (SELECT cb.cod_barra FROM cod_barras cb WHERE cb.codigo = ic.produto LIMIT 1) as cod_barra,
                            p.descricao,
                            ic.quantidade,
                            ic.valor,
                            ic.desconto,
                            p.prc_venda  -- 7ª Coluna
                        FROM itens_cancelados ic
                        LEFT JOIN produtos p ON ic.produto = p.codigo
                        WHERE ic.n_venda = %s 
                          AND ic.empresa = %s
                          AND ic.data = %s
                """, (venda, empresa, data_db))
                itens = cur.fetchall()
                
                # Fallback de busca ampla se falhar data exata
                if not itens:
                     if log_avancado:
                        logging.warning("      ⚠️ Itens não encontrados com a data exata. Tentando busca ampla...")
                     cur.execute("""
                        SELECT ic.produto,
                                (SELECT cb.cod_barra FROM cod_barras cb WHERE cb.codigo = ic.produto LIMIT 1),
                                p.descricao, ic.quantidade, ic.valor, ic.desconto, 
                                p.prc_venda -- 7ª Coluna
                        FROM itens_cancelados ic
                        LEFT JOIN produtos p ON ic.produto = p.codigo
                        WHERE ic.n_venda = %s AND ic.empresa = %s
                    """, (venda, empresa))
                     itens_todos = cur.fetchall()
                     
                     # Tenta encontrar apenas os itens que somam o valor do cupom atual
                     itens = []
                     soma_alvo = float(valor_total_cupom or 0)
                     for it in itens_todos:
                         val_item = float(it[4] or 0) * float(it[3] or 0)
                         if abs(val_item - soma_alvo) < 0.05:
                             itens.append(it)
                             break
                     if not itens:
                         itens = itens_todos

                if not itens:
                    # QUERY 4: Fallback Devolvidos no Cancelamento Real (7 colunas)
                    cur.execute("""
                        SELECT d.produto,
                               (SELECT cb.cod_barra FROM cod_barras cb WHERE cb.codigo = d.produto LIMIT 1),
                               p.descricao, d.quanti, d.preco, 
                               0,            -- 6ª Coluna
                               p.prc_venda   -- 7ª Coluna
                        FROM devolvidos d
                        LEFT JOIN produtos p ON d.produto = p.codigo
                        WHERE d.venda = %s AND d.empresa = %s
                    """, (venda, empresa))
                    itens = cur.fetchall()
                
                if not itens:
                    # Se mesmo assim não achar, tenta logar erro detalhado
                    raise ValueError(f"Cancelamento {venda} não possui itens válidos.")
                
                cancelada_flag = True
            
            # Processar itens de cancelamento/devolução
            for item_row in itens:
                (prod, cod_barra, desc, qtd, valor_liquido_db, desconto_percentual_db, preco_cadastro_db) = item_row

                # 2. DEFINIR QTD_VAL
                qtd_val = float(qtd or 0.0)
                
                # 3. DEFINIR PREÇOS
                preco_liquido = float(valor_liquido_db or 0.0)   
                preco_bruto = float(preco_cadastro_db or 0.0)    

                if preco_bruto <= 0:
                    preco_bruto = preco_liquido

                importe_unitario_payload = preco_bruto
                
                val_bruto_total = preco_bruto * qtd_val
                val_liquido_total = preco_liquido * qtd_val
                
                diff_total = val_bruto_total - val_liquido_total

                desconto_item = 0.0
                recargo_item = 0.0

                if diff_total > 0.005:
                    desconto_item = diff_total
                elif diff_total < -0.005:
                    recargo_item = abs(diff_total)
                
                importe_linha_payload = val_liquido_total

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
        
        else:
            # VENDA NORMAL
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
            
            soma_quanti = sum(float(i[2]) for i in itens)
            soma_devolvido = sum(float(i[5] or 0) for i in itens)
            
            if soma_quanti > 0 and soma_quanti == soma_devolvido:
                cancelada_flag = True
            
            for item_row in itens:
                (prod, desc, qtd, precodb, precovendadb, dev, 
                cod_barra, acrescimo_db, precovenda_cadastro_db) = item_row

                qtd_val = float(qtd or 0.0)
                desconto_item = 0.0
                recargo_item = 0.0

                preco_base = float(precodb or 0.0)
                preco_final = float(precovendadb or 0.0)

                acrescimo = (acrescimo_db or "").strip().upper()
                if preco_final == 0 and acrescimo == 'SIM':
                     preco_final = float(precovenda_cadastro_db or 0.0)

                # 1. O Unitário no JSON será sempre o PREÇO BASE (Bruto/Original)
                importe_unitario_payload = preco_base
                
                # 2. Calcula a diferença total da linha (Base vs Final)
                # Fórmula: (Preço Base * Qtd) - (Preço Final * Qtd)
                val_base_total = preco_base * qtd_val
                val_final_total = preco_final * qtd_val
                diff_total = val_base_total - val_final_total
                
                # 3. Define se é Desconto ou Recargo
                if diff_total > 0.005: 
                    # Se Base > Final, o cliente pagou menos = DESCONTO
                    desconto_item = diff_total
                    recargo_item = 0.0
                elif diff_total < -0.005:
                    # Se Base < Final, o cliente pagou mais = RECARGO
                    desconto_item = 0.0
                    recargo_item = abs(diff_total)
                else:
                    # Preços iguais
                    desconto_item = 0.0
                    recargo_item = 0.0

                # 4. Cálculo do Importe Final da Linha (Fundamental para bater o caixa)
                # Matemática: Valor Bruto - Desconto + Recargo = Valor Líquido Pago
                importe_linha_payload = val_base_total - desconto_item + recargo_item

                # Acumula nos totais gerais da venda
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
        
        if not detalhes:
            raise ValueError(f"Nenhum item encontrado para a transação {venda}.")
        
        # Validar e ajustar diferença de itens
        soma_itens = sum(d['importe'] for d in detalhes)
        diferenca_itens = round(soma_itens - valor_total_cupom, 2)
        
        if log_avancado:
            logging.info(f"\n   📊 VALIDAÇÃO DE ITENS:")
            logging.info(f"      Soma Itens (importe): R$ {soma_itens:.2f}")
            logging.info(f"      Total Cupom: R$ {valor_total_cupom:.2f}")
            logging.info(f"      Diferença: R$ {diferenca_itens:.2f}")
        
        if abs(diferenca_itens) > 0.01 and detalhes:
            # --- ESTRATÉGIA INTELIGENTE PARA ERRO DE CANCELAMENTO PARCIAL ---
            # Verifica se a diferença é EXATAMENTE igual a algum item da lista.
            # Se for, significa que esse item veio a mais na SQL e deve ser removido.
            item_intruso_index = -1
            
            # Só faz essa verificação agressiva se a diferença for positiva (tem item sobrando)
            if diferenca_itens > 0:
                for i, item in enumerate(detalhes):
                    # Aceita uma margem de erro de 1 centavo
                    if abs(item['importe'] - diferenca_itens) <= 0.01:
                        item_intruso_index = i
                        break
            
            if item_intruso_index != -1:
                # Remove o item intruso
                removido = detalhes.pop(item_intruso_index)
                # Recalcula a diferença após a remoção (deve zerar)
                diferenca_itens = round(diferenca_itens - removido['importe'], 2)
                
                if log_avancado:
                    logging.info(f"      ✅ CORREÇÃO INTELIGENTE: Item '{removido['descripcionArticulo']}' identificado como sobra e removido.")
                    logging.info(f"      Nova Diferença: R$ {diferenca_itens:.2f}")

            # --- FIM DA ESTRATÉGIA INTELIGENTE ---

            # Se ainda houver diferença (arredondamento ou erro não identificado acima), aplica no último
            if abs(diferenca_itens) > 0.01 and detalhes:

                if diferenca_itens > 0 and diferenca_itens > detalhes[-1]['importe']:
                    if log_avancado:
                        logging.warning(f"      ⚠️ PBM Detectado: Desconto (R$ {diferenca_itens}) > Último Item. Rateando...")
                    
                    total_para_ratear = diferenca_itens
                    soma_bases = sum(d['importe'] for d in detalhes)
                    
                    for item in detalhes:
                        # Quanto esse item representa do total?
                        peso = item['importe'] / soma_bases if soma_bases > 0 else 0
                        parte_desconto = round(diferenca_itens * peso, 2)
                        
                        # Aplica desconto no item sem deixar negativo (segurança)
                        if item['importe'] - parte_desconto < 0:
                            parte_desconto = item['importe']

                        item['importe'] = round(item['importe'] - parte_desconto, 2)
                        item['descuento'] = round(item['descuento'] + parte_desconto, 2)
                        total_para_ratear -= parte_desconto
                    
                    # Se sobrou algum centavo de arredondamento, joga no último
                    if abs(total_para_ratear) > 0.001:
                         detalhes[-1]['importe'] = round(detalhes[-1]['importe'] - total_para_ratear, 2)
                         detalhes[-1]['descuento'] = round(detalhes[-1]['descuento'] + total_para_ratear, 2)

                else:
                    if diferenca_itens > 0:
                        desconto_extra = diferenca_itens
                        detalhes[-1]['importe'] = round(detalhes[-1]['importe'] - desconto_extra, 2)
                        detalhes[-1]['descuento'] = round(detalhes[-1]['descuento'] + desconto_extra, 2)
                        if log_avancado:
                            logging.info(f"      ⚠️ Ajuste fino (Desconto): R$ {desconto_extra:.2f}")
                    else:
                        recargo_extra = abs(diferenca_itens)
                        detalhes[-1]['importe'] = round(detalhes[-1]['importe'] + recargo_extra, 2)
                        detalhes[-1]['recargo'] = round(detalhes[-1]['recargo'] + recargo_extra, 2)
                        if log_avancado:
                            logging.info(f"      ⚠️ Ajuste fino (Acréscimo): R$ {recargo_extra:.2f}")
        
        numero_base = abs(int(float(cupom)))
        descricao_finalizadoras = '-'.join(finalizadoras_desc)

        # 🔥 REMOVER ITENS COM DESCONTO 100% (API não aceita itens com importe = 0)
        if detalhes:
            detalles_filtrados = []
            
            for item in detalhes:
                importe_final = item['importe']
                if importe_final < 0.01:
                    if log_avancado:
                        logging.warning(f"      ⚠️ Item removido (importe = R$ {importe_final:.2f}): {item['descripcionArticulo']}")
                else:
                    detalles_filtrados.append(item)
            
            detalhes = detalles_filtrados
            
        # Garantir que pelo menos 1 item permaneça
        if not detalhes:
            raise ValueError(f"Venda {venda}: Todos os itens foram removidos. Não é possível enviar venda sem itens.")

        numero_base = abs(int(float(cupom)))
        descricao_finalizadoras = '-'.join(finalizadoras_desc)

        desconto_calculado_seguro = sum(d['descuento'] for d in detalhes)
        recargo_calculado_seguro = sum(d['recargo'] for d in detalhes)

        payload = {
            "fecha": dt_iso,
            "numero": f"-{str(numero_base).zfill(8)}" if cancelada_flag else str(numero_base).zfill(8),
            "total": abs(valor_total_cupom),
            "codigoMoneda": "986",
            "cotizacion": 1.00,
            "descuentoTotal": round(desconto_calculado_seguro, 2), # ✅ SOMA SEGURA
            "recargoTotal": round(recargo_calculado_seguro, 2),    # ✅ SOMA SEGURA
            "cancelacion": cancelada_flag,
            "codigoCanalVenta": canal_codigo,
            "descripcionCanalVenta": f"{canal_descricao}-{descricao_finalizadoras}",
            "detalles": detalhes,
            "pagos": pagamentos
        }
        
        if log_avancado:
            logging.info(f"\n   🎯 PAYLOAD FINAL:")
            logging.info(json.dumps(payload, indent=2, ensure_ascii=False))
            
            soma_final_pagos = sum(p['importe'] for p in pagamentos)
            soma_final_itens = sum(d['importe'] for d in detalhes)
            
            logging.info(f"\n   ✅ VALIDAÇÃO FINAL:")
            logging.info(f"      Total do cupom: R$ {valor_total_cupom:.2f}")
            logging.info(f"      Soma itens: R$ {soma_final_itens:.2f}")
            logging.info(f"      Desconto Total: R$ {desconto_calculado_seguro:.2f}")
            logging.info(f"      Recargo Total: R$ {recargo_calculado_seguro:.2f}")
            logging.info(f"{'='*80}\n")
        
        return payload
        
    except (ValueError, Exception) as e:
        logging.error(f"❌ Erro crítico ao montar payload para venda {venda}: {e}")
        raise
    finally:
        if conn and not conn.closed:
            if cur: cur.close()
            conn.close()