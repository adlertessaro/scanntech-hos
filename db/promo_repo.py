from datetime import datetime
import uuid
from .conexao import conectar
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _gerar_uuid():
    """Gera um ID único no formato UUID."""
    return str(uuid.uuid4())

def _buscar_ou_criar_cabecalho(cur, promo_scanntech):
    """Verifica se o cabeçalho da promoção já existe. Se não, cria um novo."""
    id_promo_scanntech = str(promo_scanntech.get("id"))
    cur.execute("SELECT id FROM promocao_cab WHERE id_scanntech = %s", (id_promo_scanntech,))
    resultado = cur.fetchone()
    
    if resultado:
        id_cab_final = resultado[0]
        logging.info(f"  -> Cabeçalho da promoção Scanntech {id_promo_scanntech} já existe. Reutilizando ID do sistema: {id_cab_final}")
        return id_cab_final
    else:
        id_cab_uuid = _gerar_uuid()
        now_timestamp = datetime.now()
        cur.execute("""
            INSERT INTO promocao_cab (
                id, descricao, vigencia_inicial, vigencia_final,
                dias_semana, dias_mes, operador_alteracao,
                created_at, updated_at, deleted_at, tipo_venda, id_scanntech
            ) VALUES (%s, %s, %s, %s, 127, '1111111111111111111111111111111', -530, %s, %s, NULL, 7, %s)
        """, (
            id_cab_uuid, promo_scanntech.get("descripcion", ""),
            promo_scanntech.get("vigenciaDesde", "")[:10] if promo_scanntech.get("vigenciaDesde") else None,
            promo_scanntech.get("vigenciaHasta", "")[:10] if promo_scanntech.get("vigenciaHasta") else None,
            now_timestamp, now_timestamp, id_promo_scanntech
        ))
        logging.info(f"  -> Promoção Scanntech {id_promo_scanntech} nova. Inserindo cabeçalho com ID do sistema: {id_cab_uuid}")
        return id_cab_uuid

def _vincular_loja_ao_cabecalho(cur, id_cab_promocao, empresa_erp):
    """Insere o vínculo da promoção com a loja, se ainda não existir."""
    cur.execute("""
        INSERT INTO promocao_cab_lojas (id_cab_promocao, empresa) VALUES (%s, %s)
        ON CONFLICT (id_cab_promocao, empresa) DO NOTHING;
    """, (id_cab_promocao, empresa_erp))

def _buscar_produto_no_erp(cur, cod_barras):
    """Busca os dados de um produto no banco local pelo código de barras."""
    cur.execute("SELECT p.codigo, p.descricao, p.prc_venda FROM produtos p JOIN cod_barras cb ON p.codigo = cb.codigo WHERE cb.cod_barra = %s", (cod_barras,))
    return cur.fetchone()

def _preparar_regras_promocao(tipo_promo, item, detalhes, produto_erp):
    """
    Calcula os valores da promoção com base no seu tipo e nas novas regras de negócio.
    Esta função precisa do 'produto_erp' para acessar o 'prc_venda'.
    """
    _, _, prc_venda = produto_erp
    prc_venda = float(prc_venda or 0.0)

    # Dicionário base para as regras
    regras = {
        "desconto": 0.0,
        "levex": None,
        "paguey": None,
        "por_valor": 0,
        "valor": 0.0,
        "quantidade": None  # Campo 'quantidade' será sempre nulo conforme a regra
    }

    quantidade_api = item.get("cantidad", 1)

    if tipo_promo == "PRECIO_FIJO":
        regras["por_valor"] = 0
        # Para 'promocao_prd', 'levex' só é preenchido se for maior que 1
        if quantidade_api > 1:
            regras["levex"] = quantidade_api

        preco_fixo_api = float(detalhes.get("precio", 0.0))
        if prc_venda > 0 and preco_fixo_api > 0 and quantidade_api > 0:
            preco_unitario_promo = preco_fixo_api / quantidade_api
            regras["valor"] = preco_unitario_promo
            # Cálculo de desconto para 'promocao_prd_lojas'
            desconto_percent = ((prc_venda - preco_unitario_promo) * 100) / prc_venda
            regras["desconto"] = max(0, desconto_percent) # Garante que o desconto não seja negativo

    elif tipo_promo == "LLEVA_PAGA":
        paga_api = detalhes.get("paga")
        regras["levex"] = quantidade_api
        regras["paguey"] = paga_api

        if paga_api is not None and prc_venda > 0 and quantidade_api > 0:
            # Cálculo de valor unitário efetivo para 'promocao_prd' e 'promocao_prd_lojas'
            valor_unitario_efetivo = (prc_venda * paga_api) / quantidade_api
            regras["valor"] = valor_unitario_efetivo
            
            # O desconto percentual total da promoção "Leve X Pague Y" é (X-Y)/X * 100
            desconto_percent = ((quantidade_api - paga_api) * 100) / quantidade_api
            regras["desconto"] = max(0, desconto_percent)

    elif tipo_promo == "DESCUENTO_VARIABLE":
        desconto_api = detalhes.get("descuento", 0.0)
        regras["levex"] = quantidade_api
        regras["desconto"] = desconto_api
        
        if prc_venda > 0:
            # Cálculo do valor unitário com o desconto aplicado
            valor_unitario_efetivo = prc_venda * (1 - (desconto_api / 100))
            regras["valor"] = valor_unitario_efetivo

    return regras


def _inserir_ou_atualizar_produto_promocao(cur, id_cab_final, produto_erp, empresa_erp, regras):
    """
    Verifica se o produto já está na promoção.
    Se sim, ATUALIZA. Se não, INSERE.
    """
    codigo_interno, _, _ = produto_erp
    now_timestamp = datetime.now()

    cur.execute(
        "SELECT id FROM promocao_prd WHERE produto = %s AND id_cab_promocao = %s",
        (codigo_interno, id_cab_final)
    )
    resultado = cur.fetchone()

    id_prd_final = None
    if resultado:
        id_prd_final = resultado[0]
        logging.info(f"    -> [UPDATE PATH] Produto (cód: {codigo_interno}) já vinculado à promoção. Atualizando regras com ID de produto: {id_prd_final}")
        
        cur.execute("""
            UPDATE promocao_prd SET
                updated_at = %s,
                desconto = %s,
                levex = %s,
                paguey = %s,
                por_valor = %s,
                valor = %s
            WHERE id = %s
        """, (
            now_timestamp, 
            regras["desconto"], 
            regras["levex"],
            regras["paguey"], 
            regras["por_valor"], 
            regras["valor"], 
            id_prd_final
        ))
    else:
        id_prd_final = _gerar_uuid()
        logging.info(f"    -> [INSERT PATH] Produto (cód: {codigo_interno}) novo na promoção. Inserindo com novo ID: {id_prd_final}")
        
        cur.execute("""
            INSERT INTO promocao_prd (
                id, produto, id_cab_promocao, operador_alteracao, created_at,
                updated_at, desconto, levex, paguey, por_valor, valor
            ) VALUES (%s, %s, %s, -530, %s, %s, %s, %s, %s, %s, %s)
        """, (
            id_prd_final, codigo_interno, id_cab_final, now_timestamp, now_timestamp,
            regras["desconto"], regras["levex"], regras["paguey"],
            regras["por_valor"], regras["valor"]
        ))

    logging.info(f"    -> Vinculando produto (ID: {id_prd_final}) à loja {empresa_erp} em 'promocao_prd_lojas'.")
    
    # MODIFICADO: Os campos 'quantidade', 'desconto', 'levex', 'paguey' são atualizados.
    # O campo 'quantidade' recebe o valor do dicionário de regras, que agora é sempre nulo.
    cur.execute("""
        INSERT INTO promocao_prd_lojas (
            id_prd_promocao, empresa, quantidade, desconto, levex, paguey)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_prd_promocao, empresa) DO UPDATE SET
            quantidade = EXCLUDED.quantidade,
            desconto = EXCLUDED.desconto,
            levex = EXCLUDED.levex,
            paguey = EXCLUDED.paguey;
    """, (
        id_prd_final, empresa_erp, regras["quantidade"], regras["desconto"],
        regras["levex"], regras["paguey"]
    ))

def _remover_vinculos_obsoletos(cur, empresa_erp, ids_promocoes_api):
    """
    Remove os vínculos de promoções que existem no banco para uma loja, 
    mas que não vieram mais na carga da API.
    IGNORA completamente promoções com id_scanntech NULO.
    """
    logging.info(f"  -> Iniciando sincronização: Verificando promoções a serem removidas para a empresa {empresa_erp}.")

    cur.execute("""
        SELECT DISTINCT c.id_scanntech 
        FROM promocao_cab c
        JOIN promocao_cab_lojas cl ON c.id = cl.id_cab_promocao
        WHERE cl.empresa = %s AND c.id_scanntech IS NOT NULL
    """, (empresa_erp,))
    
    ids_no_banco = {str(row[0]) for row in cur.fetchall()}
    ids_api_set = {str(pid) for pid in ids_promocoes_api if pid is not None}
    ids_a_remover = ids_no_banco - ids_api_set

    if not ids_a_remover:
        logging.info(f"  -> Sincronização finalizada. Nenhuma promoção obsoleta encontrada para a empresa {empresa_erp}.")
        return

    logging.warning(f"  -> PROMOÇÕES OBSOLETAS PARA EMPRESA {empresa_erp}: {ids_a_remover}. Removendo vínculos...")

    placeholders = ','.join(['%s'] * len(ids_a_remover))
    cur.execute(f"""
        SELECT pp.id, pc.id FROM promocao_prd pp
        JOIN promocao_cab pc ON pp.id_cab_promocao = pc.id
        JOIN promocao_cab_lojas pcl ON pc.id = pcl.id_cab_promocao
        WHERE pcl.empresa = %s AND pc.id_scanntech IN ({placeholders})
    """, (empresa_erp, *list(ids_a_remover)))
    
    ids_internos_para_limpeza = cur.fetchall()
    if not ids_internos_para_limpeza:
        logging.warning("  -> Nenhum vínculo interno encontrado para os IDs obsoletos. Nenhuma remoção necessária.")
        return

    ids_prd_promocao = [item[0] for item in ids_internos_para_limpeza]
    ids_cab_promocao = list(set([item[1] for item in ids_internos_para_limpeza]))

    cur.execute("DELETE FROM promocao_prd_lojas WHERE empresa = %s AND id_prd_promocao = ANY(%s)", (empresa_erp, ids_prd_promocao))
    logging.info(f"    -> Removidos {cur.rowcount} vínculos de produtos em 'promocao_prd_lojas'.")
    
    cur.execute("DELETE FROM promocao_cab_lojas WHERE empresa = %s AND id_cab_promocao = ANY(%s)", (empresa_erp, ids_cab_promocao))
    logging.info(f"    -> Removidos {cur.rowcount} vínculos de cabeçalhos em 'promocao_cab_lojas'.")


def salvar_e_processar_promocoes(todas_promocoes_por_loja):
    """Orquestra a sincronização de promoções: insere, atualiza e remove vínculos com as lojas."""
    conn = None
    try:
        conn = conectar()
        cur = conn.cursor()
        
        if not todas_promocoes_por_loja:
            logging.warning("O dicionário 'todas_promocoes_por_loja' está vazio. Nenhuma promoção para processar.")
            return

        for empresa_erp, promocoes_da_loja in todas_promocoes_por_loja.items():
            logging.info(f"\n============================================================")
            logging.info(f"Processando {len(promocoes_da_loja)} promoções para a EMPRESA ERP: {empresa_erp}...")
            
            if not promocoes_da_loja:
                logging.warning(f"A lista de promoções para a empresa {empresa_erp} está vazia. Pulando para a próxima.")
                continue

            for promo in promocoes_da_loja:
                id_promo_scanntech = promo.get('id', 'ID_N/A')
                logging.info(f"\n--- Processando Promoção Scanntech ID: {id_promo_scanntech} ---")
                
                id_cab_final = _buscar_ou_criar_cabecalho(cur, promo)
                _vincular_loja_ao_cabecalho(cur, id_cab_final, empresa_erp)

                detalhes = promo.get("detalles", {})
                condiciones = detalhes.get("condiciones", {})
                items = condiciones.get("items", [])

                if not items:
                    logging.warning(f"Promoção {id_promo_scanntech} não possui a lista de 'items'. Pulando.")
                    continue

                for item in items:
                    articulos = item.get("articulos", [])
                    if not articulos:
                        logging.warning(f"Dentro da promoção {id_promo_scanntech}, um 'item' não possui 'articulos'. Pulando.")
                        continue
                        
                    for art in articulos:
                        cod_barras = art.get("codigoBarras")
                        if not cod_barras: continue

                        produto_erp = _buscar_produto_no_erp(cur, cod_barras)
                        
                        if not produto_erp:
                            logging.warning(f"    -> AVISO: Produto com EAN {cod_barras} (da promoção {id_promo_scanntech}) não foi encontrado. Produto NÃO será inserido.")
                            continue
                        
                        # MODIFICADO: A função de cálculo agora é chamada AQUI, pois temos o produto_erp
                        regras = _preparar_regras_promocao(promo.get("tipo"), item, detalhes, produto_erp)
                        
                        logging.info(f"    -> SUCESSO: Produto com EAN {cod_barras} encontrado no ERP (Cód: {produto_erp[0]}). Prosseguindo com regras: {regras}")
                        _inserir_ou_atualizar_produto_promocao(cur, id_cab_final, produto_erp, empresa_erp, regras)

            ids_promocoes_api = [p.get("id") for p in promocoes_da_loja]
            _remover_vinculos_obsoletos(cur, empresa_erp, ids_promocoes_api)

        conn.commit()
        logging.info("\n💾 Processamento de todas as promoções finalizado com sucesso.")
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"❌ Erro fatal durante o processamento das promoções: {e}", exc_info=True)
        raise e
    finally:
        if conn:
            if cur: cur.close()
            conn.close()