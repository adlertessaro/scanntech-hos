# scanntech/services/processors/vendas_processor.py

import json
from datetime import datetime
from scanntech.db.conexao import conectar
from scanntech.api.scanntech_api_vendas import enviar_vendas_lote
from scanntech.services.payloads.vendas_payload import montar_payload_da_venda

# ==============================================================================
# FUNÇÕES AUXILIARES
# (Estas funções não devem ser alteradas)
# ==============================================================================

def limitar_codigo_caixa(nrcaixa):
    """Limita o número do caixa a 5 dígitos."""
    try:
        if isinstance(nrcaixa, float):
            nrcaixa = int(nrcaixa)
        nrcaixa_str = ''.join(c for c in str(nrcaixa) if c.isdigit())
        return nrcaixa_str[-5:] if len(nrcaixa_str) > 5 else nrcaixa_str.zfill(5)
    except (TypeError, ValueError):
        return "00001"

def verificar_venda_ja_processada(cur, venda, empresa, tipo_evento):
    """Verifica se um evento específico para uma venda já foi logado com sucesso."""
    try:
        cur.execute("""
            SELECT COUNT(*) FROM int_scanntech_vendas_logs
            WHERE venda = %s AND empresa = %s AND tipo_evento = %s
        """, (venda, empresa, tipo_evento))
        return cur.fetchone()[0] > 0
    except Exception as e:
        print(f"❌ Erro ao verificar evento '{tipo_evento}' da venda {venda}: {e}")
        return False

def excluir_venda_da_fila(cur, venda, empresa, nrcaixa):
    """Executa o SQL para excluir uma venda da fila de processamento."""
    try:
        cur.execute("""
            DELETE FROM int_scanntech_vendas
            WHERE venda = %s AND empresa = %s AND nrcaixa = %s
        """, (venda, empresa, nrcaixa))
        return True
    except Exception as e:
        print(f"❌ Erro ao executar DELETE para a venda {venda}: {e}")
        return False

def inserir_log_de_sucesso(cur, venda, empresa, nrcaixa, id_lote, tipo_evento):
    """Executa o SQL para inserir um log de sucesso."""
    try:
        cur.execute("""
            INSERT INTO int_scanntech_vendas_logs 
            (venda, empresa, nrcaixa, data_hora_retorno, id_lote, tipo_evento)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (venda, empresa, tipo_evento) DO UPDATE SET
                data_hora_retorno = EXCLUDED.data_hora_retorno,
                id_lote = EXCLUDED.id_lote,
                nrcaixa = EXCLUDED.nrcaixa
        """, (venda, empresa, nrcaixa, datetime.now(), id_lote, tipo_evento))
        return True
    except Exception as e:
        print(f"❌ Erro ao executar INSERT no log para o evento '{tipo_evento}' da venda {venda}: {e}")
        return False

# ==============================================================================
# FUNÇÃO PRINCIPAL DE PROCESSAMENTO
# ==============================================================================

def processar_envio_vendas(config):
    """
    Processa a fila de vendas, enviando-as em lotes para a API Scanntech.
    Inclui lógica para reconstruir vendas a partir de cancelamentos, se necessário,
    e gerencia as transações de banco de dados de forma atômica para evitar loops.
    """
    conn = None
    cur = None
    try:
        empresa_config = int(config['empresa'])
        print(f"🛠️  Iniciando processamento para empresa configurada: {empresa_config}")

        conn = conectar()
        cur = conn.cursor()

        # 1. BUSCA OS GRUPOS DE TRABALHO (EMPRESA/CAIXA)
        cur.execute("""
            SELECT empresa, nrcaixa
            FROM int_scanntech_vendas
            WHERE tentativas < 3 AND empresa = %s
            GROUP BY empresa, nrcaixa
        """, (empresa_config,))
        grupos = cur.fetchall()
        if not grupos:
            print("✅ Nenhuma transação pendente encontrada para a empresa.")
            return

        print(f"🧪 Grupos de trabalho encontrados: {grupos}")
        
        # 2. ITERA SOBRE CADA GRUPO
        for empresa, nrcaixa in grupos:
            empresa = int(empresa)
            nrcaixa_original_int = int(nrcaixa)
            nrcaixa_limitado = limitar_codigo_caixa(nrcaixa_original_int)
            print(f"\n🔄 Processando grupo: Empresa {empresa}, Caixa {nrcaixa_limitado}")

            # 3. PROCESSA VENDAS EM LOTES DENTRO DE UM GRUPO
            while True:
                try:
                    cur.execute("""
                        SELECT venda FROM int_scanntech_vendas
                        WHERE empresa = %s AND nrcaixa = %s AND tentativas < 3
                        ORDER BY data_hora_inclusao LIMIT 350
                    """, (empresa, nrcaixa_original_int))
                    vendas = [int(row[0]) for row in cur.fetchall()]

                    if not vendas:
                        print(f"✅ Fim do processamento para o grupo Empresa {empresa}, Caixa {nrcaixa_limitado}.")
                        break # Sai do 'while True' e vai para o próximo grupo

                    print(f"🧾 Lote selecionado com {len(vendas)} transações.")
                    payloads = []
                    vendas_enviadas = {}
                    
                    # 4. ITERA SOBRE CADA VENDA INDIVIDUAL DO LOTE
                    for venda in vendas:
                        try:
                            cur.execute("SELECT lancamen, cupom, valor FROM caixa WHERE venda = %s AND empresa = %s", (venda, empresa))
                            row = cur.fetchone()
                            if not row:
                                raise ValueError(f"Dados não encontrados na tabela 'caixa' para a venda {venda}.")

                            lancamen, cupom, valor = row
                            is_devolucao = lancamen in ('CC', 'DV')
                            tipo_evento = lancamen if is_devolucao else 'VENDA'

                            # 5. LÓGICA DE RECONSTRUÇÃO DE VENDA (SE FOR UM CANCELAMENTO)
                            if is_devolucao:
                                if not verificar_venda_ja_processada(cur, venda, empresa, 'VENDA'):
                                    print(f"⚠️  Dependência: Venda {venda} não foi enviada. Reconstruindo a partir do cancelamento...")
                                    payload_venda_reconstruida = montar_payload_da_venda(
                                        venda, empresa, config, nrcaixa_limitado,
                                        is_devolucao=True, force_as_sale=True,
                                        cupom=cupom, valor_total=valor
                                    )
                                    if not payload_venda_reconstruida:
                                        raise ValueError("Falha ao montar payload da venda reconstruída.")
                                    
                                    resposta_venda = enviar_vendas_lote(config, nrcaixa_limitado, [payload_venda_reconstruida])
                                    
                                    if resposta_venda.get("status_code") == 200 and not (resposta_venda.get("dados", {}).get("errores")):
                                        id_lote_venda = resposta_venda.get("dados", {}).get("idLote", "desconhecido")
                                        print(f"✅ SUCESSO: Venda {venda} reconstruída e enviada (Lote {id_lote_venda}).")
                                        inserir_log_de_sucesso(cur, venda, empresa, nrcaixa_original_int, id_lote_venda, 'VENDA')
                                        conn.commit() # Comita ESTA OPERAÇÃO ISOLADAMENTE para garantir o estado
                                    else:
                                        raise Exception(f"API recusou a venda reconstruída: {resposta_venda.get('mensagem')}")

                            # 6. LÓGICA DE LIMPEZA DE GATILHOS ÓRFÃOS
                            if verificar_venda_ja_processada(cur, venda, empresa, tipo_evento):
                                print(f"📋 Limpeza: Evento '{tipo_evento}' da venda {venda} já está no log. Removendo da fila.")
                                excluir_venda_da_fila(cur, venda, empresa, nrcaixa_original_int)
                                conn.commit() # Comita ESTA OPERAÇÃO ISOLADAMENTE para remover o gatilho órfão
                                continue # Pula para a próxima venda do lote

                            # 7. MONTAGEM DO PAYLOAD PARA O LOTE PRINCIPAL
                            # ALTERADO: O caixa de envio do item será sempre o do grupo, mesmo para devoluções.
                            nrcaixa_envio_item = nrcaixa_limitado
                            payload = montar_payload_da_venda(
                                venda, empresa, config, nrcaixa_envio_item,
                                is_devolucao=is_devolucao, cupom=cupom, valor_total=valor
                            )
                            if payload:
                                payloads.append(payload)
                                vendas_enviadas[venda] = tipo_evento

                        except Exception as erro_individual:
                            print(f"❌ Erro individual na venda {venda}: {erro_individual}")
                            conn.rollback() # Desfaz qualquer operação pendente para esta venda
                            cur.execute("""
                                UPDATE int_scanntech_vendas SET tentativas = tentativas + 1, erro = %s, data_hora_tentativa = %s
                                WHERE venda = %s AND empresa = %s AND nrcaixa = %s
                            """, (str(erro_individual)[:255], datetime.now(), venda, empresa, nrcaixa_original_int))
                            conn.commit() # Comita apenas a atualização do erro
                            continue

                    # 8. ENVIO DO LOTE E PROCESSAMENTO ATÔMICO DO RESULTADO
                    if not payloads:
                        continue

                    print(f"🚀 Enviando lote de {len(payloads)} transações...")
                    # ALTERADO: O caixa de envio do lote será sempre o do grupo.
                    nrcaixa_envio_lote = nrcaixa_limitado
                    resposta_lote = enviar_vendas_lote(config, nrcaixa_envio_lote, payloads)
                    status = resposta_lote.get("status_code")
                    dados = resposta_lote.get("dados", {})

                    if status == 200:
                        id_lote = dados.get("idLote", "desconhecido")
                        erros_api = dados.get("errores", [])
                        print(f"✅ Lote enviado (ID: {id_lote}). {len(erros_api)} erro(s) reportado(s) pela API.")
                        
                        try:
                            vendas_com_erro_api = {int(e.get("numero", "0").lstrip('-').lstrip('0')) for e in erros_api}
                            
                            for venda, tipo_evento in vendas_enviadas.items():
                                if venda not in vendas_com_erro_api: # Sucesso
                                    inserir_log_de_sucesso(cur, venda, empresa, nrcaixa_original_int, id_lote, tipo_evento)
                                    excluir_venda_da_fila(cur, venda, empresa, nrcaixa_original_int)
                            
                            for erro in erros_api:
                                venda_id = int(erro.get("numero", "0").lstrip('-').lstrip('0'))
                                msg = erro.get("error", {}).get("message", "Erro desconhecido retornado pela API")
                                if venda_id in vendas_enviadas:
                                    cur.execute("""
                                        UPDATE int_scanntech_vendas SET tentativas = tentativas + 1, erro = %s, data_hora_tentativa = %s
                                        WHERE venda = %s AND empresa = %s
                                    """, (msg[:255], datetime.now(), venda_id, empresa))

                            conn.commit() # Comita TODAS as operações do lote (sucessos e erros) de uma vez
                            print("Database atualizado atomicamente.")

                        except Exception as db_error:
                            print(f"❌ Erro CRÍTICO ao atualizar o banco. Revertendo. Erro: {db_error}")
                            conn.rollback()
                    else:
                        erro_http = resposta_lote.get("mensagem", f"Erro HTTP {status}")
                        print(f"❌ Falha no envio do lote: {erro_http}")
                        try:
                            for venda in vendas_enviadas:
                                cur.execute("""
                                    UPDATE int_scanntech_vendas SET tentativas = tentativas + 1, erro = %s, data_hora_tentativa = %s
                                    WHERE venda = %s AND empresa = %s
                                """, (erro_http[:255], datetime.now(), venda, empresa))
                            conn.commit()
                        except Exception as db_error:
                            print(f"❌ Erro ao registrar falha HTTP no banco: {db_error}")
                            conn.rollback()

                except Exception as loop_error:
                    print(f"❌ Erro grave no loop de processamento do grupo. Revertendo. Erro: {loop_error}")
                    conn.rollback()
                    break # Sai do loop do grupo atual

    except Exception as e:
        print(f"❌ Erro GERAL e IRRECUPERÁVEL no script: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn and not conn.closed:
            if cur:
                cur.close()
            conn.close()
            print("\n🔌 Conexão com o banco de dados fechada.")