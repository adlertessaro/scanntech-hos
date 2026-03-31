from datetime import datetime
import uuid
from scanntech.db.conexao import conectar
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _gerar_uuid():
    """Gera um ID único para as tabelas do sistema."""
    return str(uuid.uuid4())

# --- CAMADA DE INTEGRAÇÃO (STAGING) ---

def _gravar_integracao_scanntech(cur, empresa_erp, promo):
    """
    Grava os dados brutos da Scanntech nas tabelas de integração.
    Aqui usamos ON CONFLICT pois são tabelas de controle.
    """
    id_promo_str = str(promo.get("id"))
    autor_desc = promo.get("autor", {}).get("descripcion")
    
    cur.execute("""
        INSERT INTO int_scanntech_promocao (
            data_envio, empresa, nome_promocao, autor, tipo, data_inicio, data_fim, id
        ) VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s::varchar)
        ON CONFLICT (id, empresa) DO UPDATE SET
            nome_promocao = EXCLUDED.nome_promocao,
            data_fim = EXCLUDED.data_fim,
            data_envio = CURRENT_DATE
    """, (
        empresa_erp,
        promo.get("titulo"),
        autor_desc,
        promo.get("tipo"),
        promo.get("vigenciaDesde")[:10] if promo.get("vigenciaDesde") else None,
        promo.get("vigenciaHasta")[:10] if promo.get("vigenciaHasta") else None,
        id_promo_str
    ))

    detalhes = promo.get("detalles", {})
    condiciones = detalhes.get("condiciones", {}).get("items", [])
    
    # Limpamos os itens de integração antigos para evitar duplicidade na tabela de apoio
    cur.execute("DELETE FROM int_scanntech_promocao_prd WHERE id_promocao = %s::varchar AND empresa = %s", (id_promo_str, empresa_erp))

    for item in condiciones:
        qtd_leva = item.get("cantidad")
        articulos = item.get("articulos", [])
        
        for art in articulos:
            cur.execute("""
                INSERT INTO int_scanntech_promocao_prd (
                    id_promocao, empresa, codigo_barras, nome_produto, 
                    quantidade_leva, quantidade_paga, preco, desconto, tipo_desconto
                ) VALUES (%s::varchar, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                id_promo_str,
                empresa_erp,
                art.get("codigoBarras"),
                art.get("nombre"),
                qtd_leva,
                detalhes.get("paga"),
                detalhes.get("precio"),
                detalhes.get("descuento"),
                promo.get("tipo")
            ))

def _marcar_como_inserido(cur, id_promocao, empresa_erp, cod_barras):
    """Atualiza a flag de controle usando casting para evitar erro de tipo."""
    cur.execute("""
        UPDATE int_scanntech_promocao_prd 
        SET inserido_na_promocao = true 
        WHERE id_promocao = %s::varchar AND empresa = %s AND codigo_barras = %s
    """, (str(id_promocao), empresa_erp, cod_barras))

# --- PROCESSAMENTO OFICIAL ---

def _buscar_ou_criar_cabecalho(cur, promo_scanntech):
    """
    Busca ou atualiza o cabeçalho. 
    Se a promoção já existe (via id_scanntech), forçamos a atualização 
    dos dados para que a Scanntech seja sempre a soberana.
    """
    id_scanntech_str = str(promo_scanntech.get("id"))
    now = datetime.now()
    titulo_api = promo_scanntech.get('titulo', '')[:38]
    titulo = f"SCANNTECH - {titulo_api}"
    
    # Verificação manual para evitar duplicados sem depender de Constraints no banco
    cur.execute("SELECT id FROM promocao_cab WHERE id_scanntech = %s::varchar", (id_scanntech_str,))
    res = cur.fetchone()
    
    if res:
        id_cab = res[0]
        logging.info(f" -> Atualizando promo existente: ID Scanntech {id_scanntech_str} (Interno: {id_cab})")
        # Atualização Soberana: Se mudou na API, muda no sistema.
        cur.execute("""
            UPDATE promocao_cab SET 
                descricao = %s, vigencia_inicial = %s, vigencia_final = %s, updated_at = %s
            WHERE id = %s
        """, (
            titulo,
            promo_scanntech.get("vigenciaDesde")[:10] if promo_scanntech.get("vigenciaDesde") else None,
            promo_scanntech.get("vigenciaHasta")[:10] if promo_scanntech.get("vigenciaHasta") else None,
            now, id_cab
        ))
        return id_cab
    
    # Se não existe, cria um novo
    id_uuid = _gerar_uuid()
    logging.info(f" -> Criando NOVA promoção: ID Scanntech {id_scanntech_str}")
    cur.execute("""
        INSERT INTO promocao_cab (
            id, descricao, vigencia_inicial, vigencia_final,
            dias_semana, dias_mes, operador_alteracao,
            created_at, updated_at, tipo_venda, id_scanntech
        ) VALUES (%s, %s, %s, %s, 127, '1111111111111111111111111111111', -531, %s, %s, 7, %s::varchar)
    """, (
        id_uuid, titulo,
        promo_scanntech.get("vigenciaDesde")[:10] if promo_scanntech.get("vigenciaDesde") else None,
        promo_scanntech.get("vigenciaHasta")[:10] if promo_scanntech.get("vigenciaHasta") else None,
        now, now, id_scanntech_str
    ))
    return id_uuid

def _preparar_regras_promocao(tipo_promo, item, detalhes, produto_erp, promo_completa):
    _, _, prc_venda = produto_erp
    prc_venda = float(prc_venda or 0.0)
    qtd_api = item.get("cantidad", 1)

    # Se cantidad == 1, quantidade entra como None
    levex = None if (qtd_api is None or qtd_api <= 1) else qtd_api

    multiplos = 1 if (levex is not None and levex > 1) else 0

    quantidade = None

    # Se limitePromocionesPorTicket == 0 ou None, quantidade_por_venda entra como None
    limite = promo_completa.get("limitePromocionesPorTicket")
    quantidade_por_venda = None if (limite is None or limite == 0) else limite

    regras = {
        "desconto": 0.0,
        "levex": levex,
        "paguey": detalhes.get("paga"),
        "valor": 0.0,
        "multiplos": multiplos,
        "quantidade": quantidade,
        "quantidade_por_venda": quantidade_por_venda 
    }

    if detalhes.get("paga") is not None:
        p_api = float(detalhes.get("paga"))
        regras["valor"] = (prc_venda * p_api) / qtd_api if qtd_api > 0 else prc_venda
        regras["desconto"] = ((qtd_api - p_api) * 100) / qtd_api if qtd_api > 0 else 0

    elif detalhes.get("descuento") is not None:
        desc_api = float(detalhes.get("descuento"))
        regras["desconto"] = desc_api
        regras["valor"] = prc_venda * (1 - (desc_api / 100))

    elif detalhes.get("precio") is not None:
        v_total = float(detalhes.get("precio"))
        regras["valor"] = v_total / qtd_api if qtd_api > 0 else v_total
        if prc_venda > 0:
            regras["desconto"] = ((prc_venda - regras["valor"]) * 100) / prc_venda

    return regras


def _inserir_ou_atualizar_produto_promocao(cur, id_cab, produto_erp, empresa_erp, regras, id_scanntech_api):
    cod_interno, _, _ = produto_erp
    now = datetime.now()
    chave_item = f"SCAN_{id_scanntech_api}_{cod_interno}"
    chave_loja = f"SCAN_{id_scanntech_api}_{cod_interno}_{empresa_erp}"

    try:
        # 1. UPSERT no Item da Promoção
        cur.execute("""
            INSERT INTO promocao_prd (
                id, produto, id_cab_promocao, levex, paguey, valor, desconto,
                multiplos, updated_at, created_at, operador_alteracao, scanntech_item_key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, -531, %s)
            ON CONFLICT (scanntech_item_key) WHERE scanntech_item_key IS NOT NULL DO UPDATE SET
                levex        = EXCLUDED.levex,
                paguey       = EXCLUDED.paguey,
                valor        = EXCLUDED.valor,
                desconto     = EXCLUDED.desconto,
                multiplos    = EXCLUDED.multiplos,
                deleted_at   = NULL,
                updated_at   = EXCLUDED.updated_at
            RETURNING id
        """, (
            _gerar_uuid(), cod_interno, id_cab, regras["levex"], regras["paguey"],
            regras["valor"], regras["desconto"], regras["multiplos"], now, now, chave_item
        ))

        row = cur.fetchone()
        if not row:
            logging.error(f"UPSERT promocao_prd não retornou ID | chave={chave_item}")
            return
        id_prd = row[0]
        logging.info(f"  ✅ promocao_prd | chave={chave_item} | id={id_prd}")

        # 2. UPSERT no Vínculo com a Loja
        cur.execute("""
            INSERT INTO promocao_prd_lojas (
                id_prd_promocao, empresa, quantidade, desconto,
                levex, paguey, quantidade_por_venda, scanntech_loja_key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)  -- quantidade agora é regras["quantidade"]
            ON CONFLICT (scanntech_loja_key) WHERE scanntech_loja_key IS NOT NULL DO UPDATE SET
                id_prd_promocao    = EXCLUDED.id_prd_promocao,
                desconto           = EXCLUDED.desconto,
                levex              = EXCLUDED.levex,
                paguey             = EXCLUDED.paguey,
                quantidade         = EXCLUDED.quantidade, 
                quantidade_por_venda = EXCLUDED.quantidade_por_venda
        """, (
            id_prd, empresa_erp, regras["quantidade"], regras["desconto"],
            regras["levex"], regras["paguey"], regras["quantidade_por_venda"], chave_loja
        ))
        logging.info(f"  ✅ promocao_prd_lojas | chave={chave_loja}")

    except Exception as e:
        logging.error(f"💥 ERRO em _inserir_ou_atualizar_produto_promocao: {e}")
        logging.error(f"   chave_item={chave_item} | id_cab={id_cab} | empresa={empresa_erp} | regras={regras}")
        raise


def _remover_vinculos_obsoletos(cur, empresa_erp, ids_promocoes_api):
    """Monitora e inativa promoções que sumiram da API."""
    ids_api_str_list = [str(pid) for pid in ids_promocoes_api if pid]
    
    cur.execute("""
        SELECT c.id_scanntech, c.id 
        FROM promocao_cab c
        JOIN promocao_cab_lojas cl ON c.id = cl.id_cab_promocao
        WHERE cl.empresa = %s AND c.id_scanntech IS NOT NULL
    """, (empresa_erp,))
    
    promos_no_banco = cur.fetchall()
    
    # --- CONSOLE LOGS DE AUDITORIA ---
    logging.info(f"🔍 [AUDITORIA REMOÇÃO] Empresa: {empresa_erp}")
    logging.info(f" > IDs na API (Vivos): {ids_api_str_list}")
    logging.info(f" > IDs no Banco para esta empresa: {[p[0] for p in promos_no_banco]}")
    
    for id_scan, id_interno in promos_no_banco:
        if str(id_scan) not in ids_api_str_list:
            logging.warning(f" ⚠️ INATIVANDO: Promoção {id_scan} não retornou na API. Zerando quantidade no sistema.")
            
            cur.execute("""
                UPDATE promocao_prd_lojas 
                SET quantidade = 0 
                WHERE empresa = %s AND id_prd_promocao IN (
                    SELECT id FROM promocao_prd WHERE id_cab_promocao = %s
                )
            """, (empresa_erp, id_interno))
        else:
            logging.info(f" ✅ MANTENDO: Promoção {id_scan} continua ativa na API.")

def salvar_e_processar_promocoes(todas_promocoes_por_loja):
    conn = None
    try:
        conn = conectar()
        cur = conn.cursor()
        
        # Log de entrada geral
        logging.info(f"🚀 Iniciando processamento de {len(todas_promocoes_por_loja)} lojas.")

        for empresa_erp, promocoes in todas_promocoes_por_loja.items():
            logging.info(f"--- Processando Loja ERP: {empresa_erp} | Promoções recebidas: {len(promocoes)} ---")
            
            ids_vivos_na_api = []
            for promo in promocoes:
                p_id = promo.get("id")
                ids_vivos_na_api.append(p_id)
                
                _gravar_integracao_scanntech(cur, empresa_erp, promo)
                id_cab = _buscar_ou_criar_cabecalho(cur, promo)
                
                cur.execute("SELECT 1 FROM promocao_cab_lojas WHERE id_cab_promocao = %s AND empresa = %s", (id_cab, empresa_erp))
                if not cur.fetchone():
                    cur.execute("INSERT INTO promocao_cab_lojas (id_cab_promocao, empresa) VALUES (%s, %s)", (id_cab, empresa_erp))
                
                detalhes = promo.get("detalles", {}) 
                items = detalhes.get("condiciones", {}).get("items", []) 
                
                for item in items:
                    for art in item.get("articulos", []):
                        ean = art.get("codigoBarras") 
                        cur.execute("SELECT p.codigo, p.descricao, p.prc_venda FROM produtos p JOIN cod_barras cb ON p.codigo = cb.codigo WHERE cb.cod_barra = %s", (ean,))
                        prod = cur.fetchone()
                        
                        if prod:
                            regras = _preparar_regras_promocao(promo.get("tipo"), item, detalhes, prod, promo)
                            _inserir_ou_atualizar_produto_promocao(cur, id_cab, prod, empresa_erp, regras, p_id)
                            _marcar_como_inserido(cur, p_id, empresa_erp, ean)
                        else:
                            logging.warning(f" ❌ Produto EAN {ean} não encontrado no ERP para a promo {p_id}")
            
            # Chama a inativação com logs detalhados
            _remover_vinculos_obsoletos(cur, empresa_erp, ids_vivos_na_api)
        
        conn.commit()
        logging.info("💾 Sincronização finalizada com sucesso.")
    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"💥 ERRO FATAL: {e}")
        raise e
    finally:
        if conn: conn.close()