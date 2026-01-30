# scanntech/services/processors/vendas_processor.py

import json
from datetime import datetime, timedelta
import time
from scanntech.db.conexao import conectar
from scanntech.api.scanntech_api_vendas import enviar_vendas_lote
from scanntech.services.payloads.vendas_payload import montar_payload_da_venda
from scanntech.config.settings import carregar_configuracoes
import logging

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================

def limitar_codigo_estacao(estacao):
    try:
        if isinstance(estacao, float):
            estacao = int(estacao)
        estacao_str = ''.join(c for c in str(estacao) if c.isdigit())
        return estacao_str[-5:] if len(estacao_str) > 5 else estacao_str.zfill(5)
    except (TypeError, ValueError):
        return "00001"

def verificar_venda_ja_processada(cur, venda, empresa, tipo_evento):
    try:
        cur.execute("""
            SELECT COUNT(*) FROM int_scanntech_vendas_logs
            WHERE venda = %s AND empresa = %s AND tipo_evento = %s
        """, (venda, empresa, tipo_evento))
        return cur.fetchone()[0] > 0
    except Exception as e:
        print(f"❌ Erro ao verificar evento '{tipo_evento}' da venda {venda}: {e}")
        return False

def excluir_venda_da_fila(cur, venda, empresa, estacao):
    try:
        cur.execute("""
            DELETE FROM int_scanntech_vendas
            WHERE venda = %s AND empresa = %s AND estacao = %s
        """, (venda, empresa, estacao))
        return True
    except Exception as e:
        print(f"❌ Erro ao executar DELETE para a venda {venda}: {e}")
        return False

def inserir_log_de_sucesso(cur, venda, empresa, estacao, id_lote, tipo_evento, valor_enviado=None):
    try:
        cur.execute("""
            INSERT INTO int_scanntech_vendas_logs
            (venda, empresa, estacao, data_hora_retorno, id_lote, tipo_evento, valor_enviado)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (venda, empresa, tipo_evento) DO UPDATE SET
            data_hora_retorno = EXCLUDED.data_hora_retorno,
            id_lote = EXCLUDED.id_lote,
            estacao = EXCLUDED.estacao,
            valor_enviado = EXCLUDED.valor_enviado
        """, (venda, empresa, estacao, datetime.now(), id_lote, tipo_evento, valor_enviado))
        return True
    except Exception as e:
        print(f"❌ Erro ao executar INSERT no log para o evento '{tipo_evento}' da venda {venda}: {e}")
        return False

# ==============================================================================
# FUNÇÃO PRINCIPAL DE PROCESSAMENTO
# ==============================================================================

def processar_envio_vendas():
    """
    Processa a fila de vendas para todas as empresas configuradas.
    Respeita a data de início configurada no configurador.
    """
    conn = None
    cur = None
    
    try:
        configs = carregar_configuracoes()
        config_geral = configs.get('geral', {})
        lojas = configs.get('lojas', [])
        
        if not lojas:
            logging.info("Nenhuma loja configurada para processar vendas.")
            return
        
        # 🔥 NOVA LÓGICA DE DATA DE INÍCIO
        usar_carga = config_geral.get('carga_inicial', 'false').lower() == 'true'

        if usar_carga:
            # Se habilitado, respeita a data do configurador
            data_inicio_str = config_geral.get('data_de_inicio', '')
            try:
                data_inicio = datetime.strptime(data_inicio_str, '%d/%m/%Y').date()
                logging.info(f"🚀 MODO CARGA: Respeitando data fixa: {data_inicio}")
            except:
                data_inicio = (datetime.now() - timedelta(days=7)).date()
        else:
            # Se desabilitado, respeita os 7 dias retroativos
            data_inicio = (datetime.now() - timedelta(days=7)).date()
            logging.info(f"📅 MODO NORMAL: Processando apenas últimos 7 dias (desde {data_inicio})")
        
        conn = conectar()
        cur = conn.cursor()
        
        for loja_config in lojas:
            try:
                empresa_erp = int(loja_config['empresa'])
                id_empresa_scanntech = loja_config['idempresa']
                id_local_scanntech = loja_config['idlocal']
            except KeyError as e:
                logging.error(f"❌ Configuração incompleta para a loja com ERP ID {loja_config.get('empresa', 'N/A')}. Chave ausente: {e}. Pulando esta loja.")
                continue
            
            config_completa_loja = {**config_geral, **loja_config}

            logging.info(f"📋 Config da loja {empresa_erp}: url1={config_completa_loja.get('url1', 'AUSENTE')[:30]}... usuario={config_completa_loja.get('usuario', 'AUSENTE')}")
             
            logging.info(f"\n--- Iniciando processamento de vendas para a Empresa ERP: {empresa_erp} (Scanntech ID: {id_empresa_scanntech}) ---")
            
            cur.execute("""
                SELECT estacao FROM int_scanntech_vendas
                WHERE empresa = %s
                GROUP BY estacao
            """, (empresa_erp,))
            estacoes = cur.fetchall()
            
            if not estacoes:
                logging.info(f"Nenhuma transação pendente encontrada para a empresa {empresa_erp}.")
                continue
            
            logging.info(f"Estações com pendências para a empresa {empresa_erp}: {[e[0] for e in estacoes]}")
            
            for (estacao_original,) in estacoes:
                estacao_limitada = limitar_codigo_estacao(estacao_original)
                logging.info(f"\n🔄 Processando Estação {estacao_limitada} (Original: {estacao_original})")
                
                while True:
                    try:
                        # Buscar vendas da fila
                        cur.execute("""
                            SELECT venda FROM int_scanntech_vendas
                            WHERE empresa = %s AND estacao = %s AND tentativas < 3
                            ORDER BY data_hora_inclusao LIMIT 350
                        """, (empresa_erp, estacao_original))
                        vendas_raw = [int(row[0]) for row in cur.fetchall()]
                        
                        if not vendas_raw:
                            logging.info(f"✅ Fim do processamento para a Estação {estacao_limitada}.")
                            break
                        
                        # 🔥 FILTRAR POR DATA (se configurado)
                        vendas = []
                        if data_inicio:
                            for venda in vendas_raw:
                                cur.execute("SELECT data FROM caixa WHERE venda = %s AND empresa = %s", (venda, empresa_erp))
                                row = cur.fetchone()
                                if row:
                                    data_venda = row[0]
                                    if data_venda >= data_inicio:
                                        vendas.append(venda)
                                    else:
                                        # Remove vendas antigas da fila
                                        logging.info(f"⏭️  Removendo venda {venda} da fila (data {data_venda} < {data_inicio})")
                                        excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                                        conn.commit()
                            
                            if not vendas:
                                logging.info(f"⏭️  Todas as vendas da estação {estacao_limitada} são anteriores a {data_inicio.strftime('%d/%m/%Y')}. Pulando.")
                                break
                            
                            logging.info(f"📅 Após filtro de data: {len(vendas)} vendas de {len(vendas_raw)} (>= {data_inicio.strftime('%d/%m/%Y')})")
                        else:
                            vendas = vendas_raw
                        
                        logging.info(f"🧾 Lote selecionado com {len(vendas)} transações.")
                        
                        payloads = []
                        vendas_enviadas = {}
                        
                        for venda in vendas:
                            try:
                                cur.execute("SELECT lancamen, cupom, valor, data, tipovenda FROM caixa WHERE venda = %s AND empresa = %s", (venda, empresa_erp))
                                row = cur.fetchone()
                                if not row:
                                    # Se não está no caixa, remove da fila para não travar
                                    excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                                    conn.commit()
                                    continue
                                
                                lancamen, cupom, valor, data_venda, tipovenda_db = row
                                tipovenda_srt = str(tipovenda_db or '').strip().upper()
                                lancamen_str = str(lancamen or '').strip().upper()

                                codigos_ignorar = ['EA', 'PG', 'RC', 'SA', 'TV']
                                if lancamen_str in codigos_ignorar or tipovenda_srt in codigos_ignorar:
                                    logging.info(f"⏭️ Removendo lançamento administrativo da fila: {lancamen_str}")
                                    excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                                    conn.commit()
                                    continue              

                                # Se a data da venda (mesmo que seja um cancelamento hoje) for anterior ao início
                                # configurado, NÃO enviamos. Isso evita recriar vendas antigas.
                                if data_inicio and data_venda < data_inicio:
                                    logging.info(f"👻 Fantasma detectado/Antigo: Venda {venda} (Data {data_venda}) < Início ({data_inicio}). Removendo da fila.")
                                    excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                                    conn.commit()
                                    continue

                                is_devolucao = lancamen_str in ('CC', 'DV')
                                tipo_evento = lancamen_str if is_devolucao else 'VENDA'

                                # Verifica se este evento específico já foi processado para evitar duplicidade no log
                                if verificar_venda_ja_processada(cur, venda, empresa_erp, tipo_evento):
                                    logging.info(f"⏭️ Evento {tipo_evento} da venda {venda} já enviado. Removendo da fila.")
                                    excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                                    conn.commit()
                                    continue
                                
                                payload = montar_payload_da_venda(
                                    venda, empresa_erp, config_completa_loja, estacao_limitada,
                                    is_devolucao=is_devolucao, cupom=cupom, valor_total=valor,
                                    debug_mode=False
                                )
                                
                                if payload:
                                    payloads.append(payload)
                                    vendas_enviadas[venda] = {
                                        'tipo_evento': tipo_evento,
                                        'data_venda': data_venda,
                                        'valor_total': valor,
                                        'payload': payload
                                    }
                                    
                            except Exception as erro_individual:
                                print(f"❌ Erro ao processar venda {venda}: {erro_individual}")
                                conn.rollback()
                                cur.execute("""
                                    UPDATE int_scanntech_vendas SET tentativas = tentativas + 1, erro = %s, data_hora_tentativa = %s
                                    WHERE venda = %s AND empresa = %s AND estacao = %s
                                """, (str(erro_individual)[:255], datetime.now(), venda, empresa_erp, estacao_original))
                                conn.commit()
                                continue
                        
                        if not payloads:
                            logging.info(f"⚠️  Nenhum payload válido para enviar neste lote.")
                            continue
                        
                        logging.info(f"🚀 Enviando lote de {len(payloads)} transações para a Estação {estacao_limitada}...")
                        
                        time.sleep(0.3)
                        
                        resposta_lote = enviar_vendas_lote(
                            config_completa_loja, id_empresa_scanntech, id_local_scanntech,
                            estacao_limitada, payloads
                        )
                        
                        status = resposta_lote.get("status_code")
                        dados = resposta_lote.get("dados", {})
                        
                        if status == 200:
                            id_lote = dados.get("idLote", "desconhecido")
                            erros_api = dados.get("errores", [])
                            logging.info(f"✅ Lote enviado (ID: {id_lote}). {len(erros_api)} erro(s).")
                            
                            try:
                                # 🔥 PROCESSAR ERROS RETORNADOS PELA API
                                vendas_com_erro_api = {}
                                for erro in erros_api:
                                    # Extrair número do cupom
                                    numero_str = erro.get("numero", "0")
                                    cupom_str = numero_str.lstrip('-').lstrip('0')
                                    
                                    if not cupom_str or not cupom_str.isdigit():
                                        continue
                                    
                                    cupom_int = int(cupom_str)  # 15745 (cupom)
                                    
                                    # 🔥 BUSCAR A VENDA REAL NO MAPEAMENTO
                                    venda_real = None
                                    for venda, info in vendas_enviadas.items():
                                        payload = info['payload']
                                        numero_payload = str(payload.get('numero', '')).lstrip('-').lstrip('0')
                                        if numero_payload == cupom_str:
                                            venda_real = venda  # 900224947 (ID da venda)
                                            break
                                    
                                    if not venda_real:
                                        logging.error(f"⚠️ Cupom {cupom_int} não encontrado no mapeamento de vendas enviadas!")
                                        continue
                                    
                                    # Extrair mensagem de erro
                                    error_obj = erro.get("error", {})
                                    error_code = error_obj.get("code", "ERRO_DESCONHECIDO")
                                    error_message = error_obj.get("message", "Sem mensagem")
                                    error_full = f"{error_code}: {error_message}"
                                    
                                    vendas_com_erro_api[venda_real] = error_full  # ✅ USA VENDA REAL
                                    
                                    # 🔥 LOGAR O ERRO
                                    logging.error(f"❌ Venda {venda_real} (Cupom {cupom_int}) REJEITADA:")
                                    logging.error(f"   Código: {error_code}")
                                    logging.error(f"   Mensagem: {error_message}")
                                    
                                    # Logar payload se for erro crítico
                                    if error_code in ['FALLO_MOV_SUMA_PAGOS', 'FALLO_MOV_IMPORTE_DETALLES']:
                                        logging.error(f"   📄 PAYLOAD COMPLETO:")
                                        logging.error(json.dumps(payload, indent=2, ensure_ascii=False))
                                
                                # 🔥 PROCESSAR VENDAS COM SUCESSO
                                vendas_com_sucesso = 0
                                for venda, info in vendas_enviadas.items():
                                    if venda not in vendas_com_erro_api:
                                        payload = info['payload']
                                        valor_enviado = payload['total']
                                        
                                        inserir_log_de_sucesso(cur, venda, empresa_erp, estacao_original, id_lote, info['tipo_evento'], valor_enviado)
                                        excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                                        vendas_com_sucesso += 1
                                
                                # 🔥 PROCESSAR VENDAS COM ERRO (INCREMENTAR TENTATIVAS E SALVAR ERRO)
                                for venda_id, error_msg in vendas_com_erro_api.items():
                                    cur.execute("""
                                        UPDATE int_scanntech_vendas 
                                        SET tentativas = tentativas + 1, 
                                            erro = %s, 
                                            data_hora_tentativa = %s
                                        WHERE venda = %s AND empresa = %s AND tentativas < 3
                                    """, (error_msg[:255], datetime.now(), venda_id, empresa_erp))
                                
                                conn.commit()
                                
                                # 🔥 LOG RESUMO FINAL
                                logging.info(f"")
                                logging.info(f"📊 RESUMO DO LOTE (ID: {id_lote}):")
                                logging.info(f"   ✅ Vendas aceitas: {vendas_com_sucesso}")
                                logging.info(f"   ❌ Vendas rejeitadas: {len(vendas_com_erro_api)}")
                                logging.info(f"   📦 Total no lote: {len(vendas_enviadas)}")
                                
                                if len(vendas_com_erro_api) > 0:
                                    logging.warning(f"")
                                    logging.warning(f"⚠️ ATENÇÃO: {len(vendas_com_erro_api)} venda(s) permanecem na fila com erro registrado.")
                                    logging.warning(f"   Verifique os logs acima para detalhes dos erros.")
                                
                            except Exception as db_error:
                                logging.error(f"❌ Erro ao atualizar o banco após retorno da API: {db_error}")
                                conn.rollback()

                        else:
                            # HTTP diferente de 200
                            erro_http = resposta_lote.get("mensagem", f"Erro HTTP {status}")
                            logging.error(f"❌ Falha no envio do lote: {erro_http}")
                            
                            try:
                                for venda in vendas_enviadas:
                                    cur.execute("""
                                        UPDATE int_scanntech_vendas 
                                        SET tentativas = tentativas + 1, 
                                            erro = %s, 
                                            data_hora_tentativa = %s
                                        WHERE venda = %s AND empresa = %s AND tentativas < 3
                                    """, (erro_http[:255], datetime.now(), venda, empresa_erp))
                                conn.commit()
                            except Exception as db_error:
                                logging.error(f"❌ Erro ao atualizar falha HTTP no banco: {db_error}")
                                conn.rollback()
                    
                    except Exception as loop_error:
                        logging.error(f"❌ Erro no processamento da estação {estacao_limitada}: {loop_error}")
                        if conn:
                            conn.rollback()
                        break
                
                logging.info("⏳ Aguardando 1 segundo antes da próxima estação...")
                time.sleep(1)
            
            logging.info("⏳ Aguardando 2 segundos antes da próxima loja...")
            time.sleep(2)
    
    except Exception as e:
        logging.error(f"❌ Erro GERAL no processador de vendas: {e}")
        if conn:
            conn.rollback()
    
    finally:
        if conn and not conn.closed:
            if cur:
                cur.close()
            conn.close()
            logging.info("\n🔌 Conexão com o banco foi fechada.")
