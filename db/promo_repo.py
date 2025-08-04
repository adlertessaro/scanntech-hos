from datetime import datetime
import hashlib
from scanntech.db.conexao import conectar
import logging

logger = logging.getLogger('PromocoesLogger')

def gerar_md5_id(input_str):
    return hashlib.md5(input_str.encode('utf-8')).hexdigest()

def salvar_promocoes(resultados, config):
    """
    Salva as promoções e seus produtos no banco de dados.
    A lógica foi reestruturada para:
    1. Inserir o cabeçalho da promoção (uma vez por promoção).
    2. Inserir os produtos associados.
    Isso evita erros de chave duplicada.
    """
    conn = conectar()
    cur = conn.cursor()

    empresa = config.get("empresa")
    tipos_desejados = {'LLEVA_PAGA', 'PRECIO_FIJO', 'DESCUENTO_VARIABLE', 'DESCUENTO_FIJO'}

    for promo in resultados:
        id_cab = None
        try:
            tipo = promo.get("tipo", "")
            if tipo not in tipos_desejados:
                print(f"⚠️ Promoção {promo.get('id', 'N/A')} ({tipo}) não é dos tipos desejados. Pulando...")
                continue

            id_promo = str(promo["id"])
            nome = promo.get("titulo", "")
            print(f"\n🔍 Processando promoção: {id_promo} - {nome}")
            
            # --- PASSO 1: Garantir a existência dos cabeçalhos ---
            
            # Insere na nossa tabela de log (int_scanntech_promocao)
            cur.execute("""
                INSERT INTO int_scanntech_promocao (
                    data_envio, empresa, nome_promocao, autor,
                    tipo, data_inicio, data_fim, id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id, empresa) DO NOTHING
            """, (
                datetime.now().date(), empresa, nome, promo.get("autor", {}).get("descripcion", ""), tipo,
                promo.get("vigenciaDesde", "")[:10], promo.get("vigenciaHasta", "")[:10], id_promo
            ))

            # Insere na tabela principal do sistema (promocao_cab)
            id_promocao_md5 = gerar_md5_id(f"{id_promo}-{empresa}")
            cur.execute("""
                INSERT INTO promocao_cab (
                    id, descricao, vigencia_inicial, vigencia_final,
                    dias_semana, dias_mes, operador_alteracao,
                    created_at, updated_at, deleted_at, tipo_venda, id_scanntech
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, now(), now(), NULL, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                id_promocao_md5, nome, promo.get("vigenciaDesde", "")[:10], promo.get("vigenciaHasta", "")[:10],
                127, '1111111111111111111111111111111', -530, 7, id_promo
            ))

            # Recupera o ID do cabeçalho para vincular os produtos
            cur.execute("SELECT id FROM promocao_cab WHERE id = %s", (id_promocao_md5,))
            cab = cur.fetchone()
            if not cab:
                print(f"  ⚠️ Não foi possível encontrar ou criar o cabeçalho da promoção {id_promo}. Os produtos desta promoção não serão inseridos.")
                continue
            id_cab = cab[0]

            # --- PASSO 2: Processar e inserir os produtos ---
            
            itens = promo.get("detalles", {}).get("condiciones", {}).get("items", [])
            for item in itens:
                qtd_leva = item.get("cantidad", 1)
                for art in item.get("articulos", []):
                    cod_barras = art.get("codigoBarras", "").strip()
                    nome_produto = art.get("nombre", "").strip()
                    inserido_na_promocao_principal = False

                    # Verifica se o produto existe no sistema
                    cur.execute("""
                        SELECT p.codigo FROM produtos p
                        JOIN cod_barras cb ON p.codigo = cb.codigo
                        WHERE cb.cod_barra = %s
                    """, (cod_barras,))
                    resultado_produto = cur.fetchone()

                    if resultado_produto:
                        codigo_interno = resultado_produto[0]
                        id_item_md5 = gerar_md5_id(f"{id_promo}-{cod_barras}")
                        
                        # Insere o produto na tabela principal (promocao_prd)
                        cur.execute("""
                            INSERT INTO promocao_prd (
                                id, produto, id_cab_promocao, operador_alteracao,
                                created_at, updated_at, levex, paguey
                            ) VALUES (%s, %s, %s, %s, now(), now(), %s, %s)
                            ON CONFLICT (id) DO NOTHING
                        """, (
                            id_item_md5, codigo_interno, id_cab, -530,
                            qtd_leva, promo.get("detalles", {}).get("paga", 1)
                        ))
                        print(f"  ✅ Produto {cod_barras} inserido/verificado na promocao_prd.")
                        inserido_na_promocao_principal = True
                    
                    # Insere na nossa tabela de log (int_scanntech_promocao_prd).
                    # A cláusula ON CONFLICT foi removida porque a tabela não possui a
                    # restrição UNIQUE necessária, causando o erro. O try/except geral
                    # já protege contra falhas inesperadas.
                    cur.execute("""
                        INSERT INTO int_scanntech_promocao_prd (
                            id_promocao, empresa, codigo_barras, nome_produto,
                            quantidade_leva, quantidade_paga, preco, desconto,
                            tipo_desconto, criado_em, inserido_na_promocao
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
                    """, (
                        id_promo, empresa, cod_barras, nome_produto,
                        qtd_leva, promo.get("detalles", {}).get("paga"), promo.get("detalles", {}).get("precio"),
                        promo.get("detalles", {}).get("descuento"),
                        'PERCENTUAL' if tipo == 'DESCUENTO_VARIABLE' else 'FIXO' if tipo == 'DESCUENTO_FIJO' else None,
                        inserido_na_promocao_principal
                    ))

        except Exception as e:
            print(f"❌ Erro fatal ao processar a promoção {promo.get('id', 'N/A')}: {e}")
            print("   -> Desfazendo alterações para esta promoção.")
            conn.rollback() 
            continue 

    print("\n💾 Transação finalizada.")
    conn.commit()
    cur.close()
    conn.close()

def reprocessar_produtos_pendentes():
    conn = conectar()
    cur = conn.cursor()

    # Busca promoções e produtos não processados
    cur.execute("""
        SELECT 
            p.id, p.empresa, p.nome_promocao, p.data_inicio, p.data_fim, p.tipo,
            pr.codigo_barras, pr.nome_produto, pr.quantidade_leva, pr.quantidade_paga,
            pr.preco, pr.desconto, pr.tipo_desconto
        FROM int_scanntech_promocao p
        JOIN int_scanntech_promocao_prd pr ON p.id = pr.id_promocao AND p.empresa = pr.empresa
        WHERE pr.inserido_na_promocao = FALSE
        AND p.tipo IN ('LLEVA_PAGA', 'PRECIO_FIJO', 'DESCUENTO_VARIABLE', 'DESCUENTO_FIJO')
    """)

    promocoes = cur.fetchall()
    if not promocoes:
        print("🔁 Nenhuma promoção ou produto pendente para processar.")
        cur.close()
        conn.close()
        return

    total_inseridos = 0
    promocoes_por_id = {}

    # Agrupar por promoção para evitar duplicação de cabeçalhos
    for row in promocoes:
        id_promo, empresa, nome, data_inicio, data_fim, tipo, cod_barras, nome_produto, leva, paga, preco, desconto, tipo_desconto = row
        if id_promo not in promocoes_por_id:
            promocoes_por_id[id_promo] = {
                'empresa': empresa,
                'nome': nome or f"Promoção Scanntech {id_promo}",
                'data_inicio': data_inicio or datetime.now().date(),
                'data_fim': data_fim or datetime.now().date(),
                'tipo': tipo,
                'produtos': []
            }
        promocoes_por_id[id_promo]['produtos'].append({
            'cod_barras': cod_barras,
            'nome_produto': nome_produto,
            'quantidade_leva': leva,
            'quantidade_paga': paga,
            'preco': preco,
            'desconto': desconto,
            'tipo_desconto': tipo_desconto
        })

    for id_promo, info in promocoes_por_id.items():
        empresa = info['empresa']
        id_promocao_md5 = gerar_md5_id(f"{id_promo}-{empresa}")

        # Inserir cabeçalho na promocao_cab
        try:
            cur.execute("""
                INSERT INTO promocao_cab (
                    id, descricao, vigencia_inicial, vigencia_final,
                    dias_semana, dias_mes, operador_alteracao,
                    created_at, updated_at, deleted_at, tipo_venda, id_scanntech
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), NULL, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                id_promocao_md5,
                info['nome'],
                info['data_inicio'],
                info['data_fim'],
                127,
                '1111111111111111111111111111111',
                -530,
                7,
                id_promo
            ))

            cur.execute("SELECT id FROM promocao_cab WHERE id = %s", (id_promocao_md5,))
            cab = cur.fetchone()
            if not cab:
                print(f"⚠️ Cabeçalho da promoção {id_promo} não encontrada.")
                continue
            id_cab = cab[0]

        except Exception as e:
            print(f"❌ Erro ao inserir cabeçalho da promoção {id_promo}: {e}")
            continue

        # Processar produtos
        for produto in info['produtos']:
            cod_barras = produto['cod_barras']
            leva = produto['quantidade_leva']
            paga = produto['quantidade_paga']
            preco = produto['preco']
            desconto = produto['desconto']
            tipo_desconto = produto['tipo_desconto']
            tipo_promocao = info['tipo']

            # Buscar código interno e preço de venda do produto
            cur.execute("""
                SELECT p.codigo, p.prc_venda 
                FROM produtos p
                JOIN cod_barras cb ON p.codigo = cb.codigo
                WHERE cb.cod_barra = %s
            """, (cod_barras,))
            resultado = cur.fetchone()
            if not resultado:
                print(f"⚠️ Produto com código de barras {cod_barras} não encontrado.")
                continue
            codigo_interno, prc_venda = resultado

            id_item_md5 = gerar_md5_id(f"{id_promo}-{cod_barras}")

            # Calcular desconto e valor conforme o tipo
            desconto_final = None
            levex = None
            paguey = None
            por_valor = False
            valor = None

            if tipo_promocao == 'LLEVA_PAGA' and leva and paga and leva > 0:
                levex = leva
                paguey = paga
                desconto_final = abs(((paga / leva) - 1) * 100)  # Ex.: leva 6, paga 4 -> desconto = 33.33%
            elif tipo_promocao == 'DESCUENTO_VARIABLE' and desconto and prc_venda:
                levex = leva
                desconto_final = desconto  # Ex.: 30.00%
                valor = prc_venda - (prc_venda * (desconto / 100)) if prc_venda > 0 else 0  # Ex.: prc_venda=100, desconto=30% -> valor=70
            elif tipo_promocao == 'DESCUENTO_FIJO' and desconto and prc_venda:
                levex = leva
                valor = desconto  # O desconto da API é tratado como o valor final
                desconto_final = abs(((desconto / prc_venda) - 1) * 100) if prc_venda > 0 else 0  # Ex.: desconto=10, prc_venda=15 -> desconto_final=33.33%
            elif tipo_promocao == 'PRECIO_FIJO' and preco and leva and prc_venda:
                levex = leva
                por_valor = True
                valor = preco
                desconto_final = abs(((preco / prc_venda) - 1) * 100) if prc_venda > 0 else 0  # Ex.: preco=10, prc_venda=15 -> desconto = 33.33%

            try:
                cur.execute("""
                    INSERT INTO promocao_prd (
                        id, produto, id_cab_promocao, operador_alteracao,
                        created_at, updated_at, deleted_at, desconto,
                        levex, paguey, por_valor, valor
                    ) VALUES (%s, %s, %s, %s, NOW(), NOW(), NULL, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    id_item_md5,
                    codigo_interno,
                    id_cab,
                    -530,
                    desconto_final,
                    levex,
                    paguey,
                    int(por_valor),
                    valor
                ))

                # Marcar como inserido
                cur.execute("""
                    UPDATE int_scanntech_promocao_prd
                    SET inserido_na_promocao = TRUE
                    WHERE id_promocao = %s AND codigo_barras = %s AND empresa = %s
                """, (id_promo, cod_barras, empresa))

                print(f"✅ Produto {cod_barras} inserido na promocao_prd para promoção {id_promo}.")
                total_inseridos += 1

            except Exception as e:
                print(f"❌ Erro ao inserir produto {cod_barras} na promoção {id_promo}: {e}")
                continue

    conn.commit()
    print(f"✅ {total_inseridos} produtos processados e inseridos com sucesso.")
    cur.close()
    conn.close()