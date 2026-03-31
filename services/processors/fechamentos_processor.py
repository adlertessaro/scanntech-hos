import sys
import os
from datetime import datetime, date
from scanntech.config.settings import carregar_configuracoes
from scanntech.services.payloads.fechamentos_payload import montar_payload_do_fechamento
from scanntech.api.scanntech_api_fechamentos import enviar_fechamentos_lote
from scanntech.db.conexao import conectar
from scanntech.services.processors.vendas_processor import limitar_codigo_estacao
import logging
import traceback
from scanntech.db.manutencao import limpar_dados_antigos

# Configuração básica de log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def invalidar_fechamentos_desatualizados(cur, empresa_erp):
    """
    Verifica os últimos 3 dias (mais o dia atual).
    Se a soma das vendas/cancelamentos nos LOGS (realidade atual) for diferente
    do que foi gravado na tabela de fechamentos (o que foi enviado à API),
    marca o fechamento para reenvio.
    """
    try:
        logging.info("🧹 Verificando integridade dos últimos 3 dias (Retroativo)...")
        
        # 1. Soma os logs (Realidade Aceita pela Scanntech)
        # 2. Compara com o que está gravado na tabela de fechamento
        # 3. Não usa JOIN com a tabela 'caixa' para evitar perdas
        sql_revalida = """
        UPDATE int_scanntech_fechamentos f
        SET id_lote = NULL,
            tentativas = 0, 
            erro = 'Divergência detectada entre Logs e Fechamento',
            data_hora_tentativa = NULL
        FROM (
            SELECT
                empresa,
                data_registro,
                estacao,
                SUM(CASE WHEN tipo_evento = 'VENDA' THEN ABS(valor_enviado) ELSE 0 END) as v_venda_calc,
                SUM(CASE WHEN tipo_evento IN ('CC', 'DV', 'DP') THEN ABS(valor_enviado) ELSE 0 END) as v_cancel_calc
            FROM int_scanntech_vendas_logs
            WHERE empresa = %s
              AND id_lote IS NOT NULL
              AND data_registro >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY 1, 2, 3
        ) calculo
        WHERE f.empresa = calculo.empresa
          AND f.data_fechamento = calculo.data_registro
          AND f.estacao = calculo.estacao
          AND f.id_lote IS NOT NULL -- Só revalida o que já foi enviado
          AND (
               ABS(COALESCE(f.valor_enviado_vendas, 0) - calculo.v_venda_calc) > 0.01
               OR
               ABS(COALESCE(f.valor_enviado_cancelamentos, 0) - calculo.v_cancel_calc) > 0.01
          );
        """
        cur.execute(sql_revalida, (empresa_erp,))
        
        if cur.rowcount > 0:
            logging.warning(f"♻️  DIVERGÊNCIA ENCONTRADA: {cur.rowcount} fechamentos dos últimos 3 dias foram invalidados e serão reenviados.")
        else:
            logging.info("✅ Fechamentos dos últimos 3 dias estão íntegros.")
            
    except Exception as e:
        logging.error(f"Erro na validação retroativa de fechamentos: {e}")

def processar_envio_fechamento(empresa_param=None, config_param=None):
    """
    Processa o envio de fechamentos.
    """
    conn = None
    try:
        # --- LÓGICA HÍBRIDA: MODO INTEGRADO VS MODO ISOLADO ---
        lojas_para_processar = []

        if empresa_param and config_param:
            lojas_para_processar.append({
                'empresa': int(empresa_param),
                'config': config_param
            })
        else:
            logging.info("Carregando configurações do disco (Modo Standalone)...")
            configs = carregar_configuracoes()
            config_geral = configs.get('geral', {})
            lojas_lista = configs.get('lojas', [])

            if not lojas_lista:
                logging.warning("Nenhuma loja configurada no arquivo.")
                return

            for l in lojas_lista:
                lojas_para_processar.append({
                    'empresa': int(l['empresa']),
                    'config': {**config_geral, **l}
                })

        conn = conectar()
        cur = conn.cursor()

        # 1. Primeiro, o Gerente identifica quem são os ativos
        ids_ativas = [item['empresa'] for item in lojas_para_processar]
        
        # 2. Faz a faxina
        if ids_ativas:
            logging.info(f"🧹 Realizando manutenção para empresas: {ids_ativas}")
            limpar_dados_antigos(cur, ids_ativas)
            conn.commit()
        else:
            logging.warning("⚠️ Nenhuma empresa ativa para manutenção.")

        for item in lojas_para_processar:
            empresa_erp = item['empresa']
            config_completa_loja = item['config']

            logging.info(f"🔧 Iniciando processador para Empresa {empresa_erp}...")

            # ==============================================================================
            # PASSO 0: POPULAR A TABELA AUTOMATICAMENTE (Otimizado)
            # ==============================================================================
            try:
                # Agora usamos apenas int_scanntech_vendas_logs (Verdade Única)
                sql_insert = """
                    INSERT INTO int_scanntech_fechamentos 
                    (empresa, data_fechamento, estacao, tentativas, data_hora_tentativa)
                    SELECT DISTINCT 
                        l.empresa, 
                        l.data_registro,
                        l.estacao, 
                        0, 
                        CAST(NULL AS TIMESTAMP)
                    FROM int_scanntech_vendas_logs l
                    WHERE l.empresa = %s
                      AND l.id_lote IS NOT NULL
                      AND l.data_registro < CURRENT_DATE
                      AND NOT EXISTS (
                          SELECT 1 FROM int_scanntech_fechamentos f
                          WHERE f.empresa = l.empresa 
                            AND f.data_fechamento = l.data_registro 
                            AND f.estacao = l.estacao
                      )
                """
                cur.execute(sql_insert, (empresa_erp,))
                if cur.rowcount > 0:
                    logging.info(f"🆕 Empresa {empresa_erp}: {cur.rowcount} novos fechamentos na fila.")
                conn.commit()

            except Exception as e:
                logging.error(f"Erro no Passo 0: {e}")
                conn.rollback()
            
            # ==============================================================================
            # PASSO 1: BUSCAR PENDÊNCIAS
            # ==============================================================================
            cur.execute("""
                SELECT data_fechamento, estacao
                FROM int_scanntech_fechamentos
                WHERE tentativas < 3 
                  AND id_lote IS NULL 
                  AND empresa = %s
                  AND data_fechamento < CURRENT_DATE
            """, (empresa_erp,))
            fechamentos_a_processar = cur.fetchall()

            if not fechamentos_a_processar:
                continue

            # Agrupa por estação
            fechamentos_por_estacao = {}
            for data_fechamento, estacao in fechamentos_a_processar:
                if estacao not in fechamentos_por_estacao:
                    fechamentos_por_estacao[estacao] = []
                fechamentos_por_estacao[estacao].append(data_fechamento)

            # ==============================================================================
            # PASSO 2: PROCESSAR E ENVIAR
            # ==============================================================================
            for estacao, datas in fechamentos_por_estacao.items():
                try:
                    estacao_envio = limitar_codigo_estacao(estacao)
                    logging.info(f"🚀 Enviando fechamento Empresa {empresa_erp} | Estação {estacao_envio} | {len(datas)} dias")
                    
                    payload_lote = []
                    datas_no_lote = []

                    for data_fechamento in datas:
                        payload_individual = montar_payload_do_fechamento(empresa_erp, config_completa_loja, data_fechamento, estacao)
                        
                        if payload_individual:
                            payload_lote.extend(payload_individual)
                            datas_no_lote.append(data_fechamento)
                        else:
                            logging.warning(f"⚠️ Sem dados para {data_fechamento} Estação {estacao}. Marcando erro.")
                            cur.execute("""
                                UPDATE int_scanntech_fechamentos
                                SET erro = 'Sem logs de venda válidos', tentativas = 3, data_hora_tentativa = %s
                                WHERE empresa = %s AND data_fechamento = %s AND estacao = %s
                            """, (datetime.now(), empresa_erp, data_fechamento, estacao))
                            conn.commit()

                    if not payload_lote:
                        cur.execute("""
                            UPDATE int_scanntech_fechamentos
                            SET erro = 'Sem dados para envio', tentativas = tentativas + 1, data_hora_tentativa = %s
                            WHERE empresa = %s AND estacao = %s AND data_fechamento = ANY(%s)
                        """, (datetime.now(), empresa_erp, estacao, datas_no_lote))
                        conn.commit()
                        continue

                    # Envia API
                    resposta = enviar_fechamentos_lote(config_completa_loja, estacao, payload_lote)
                    status = resposta.get("status_code")
                    dados = resposta.get("dados", {})

                    if status == 200 and not dados.get("errores"):
                        id_lote = dados.get("idLote", "enviado")
                        logging.info(f"✅ SUCESSO: Lote {id_lote} gravado. Atualizando {len(payload_lote)} registros com valores calculados...")
                        
                        # Não usamos 'dados.get(monto)', usamos o 'payload_lote' que contém o que calculamos
                        for item_payload in payload_lote:
                            data_str = item_payload['fechaVentas'] # data 'YYYY-MM-DD'
                            valor_vendas_calc = item_payload['montoVentaLiquida']
                            valor_cancel_calc = item_payload['montoCancelaciones']

                            # Atualiza registro específico daquele dia e estação com os valores do payload
                            cur.execute("""
                                UPDATE int_scanntech_fechamentos
                                SET id_lote = %s, 
                                    erro = NULL, 
                                    data_hora_tentativa = %s, 
                                    valor_enviado_vendas = %s, 
                                    valor_enviado_cancelamentos = %s
                                WHERE empresa = %s AND estacao = %s AND data_fechamento = %s
                            """, (
                                id_lote, 
                                datetime.now(), 
                                valor_vendas_calc, 
                                valor_cancel_calc, 
                                empresa_erp, 
                                estacao, 
                                data_str
                            ))
                        
                        conn.commit() # Salva tudo
                        # --------------------------------
                    else:
                        erro_msg = resposta.get("mensagem", str(dados.get("errores", "Erro desconhecido")))
                        logging.error(f"❌ FALHA API: {erro_msg}")
                        
                        cur.execute("""
                            UPDATE int_scanntech_fechamentos
                            SET tentativas = tentativas + 1, erro = %s, data_hora_tentativa = %s
                            WHERE empresa = %s AND estacao = %s AND data_fechamento = ANY(%s)
                        """, (str(erro_msg)[:255], datetime.now(), empresa_erp, estacao, datas_no_lote))
                        conn.commit()

                except Exception as e_loop:
                    logging.error(f"Erro processando estação {estacao}: {e_loop}")
                    conn.rollback()
                    try:
                        cur.execute("""
                            UPDATE int_scanntech_fechamentos
                            SET tentativas = tentativas + 1, erro = %s, data_hora_tentativa = %s
                            WHERE empresa = %s AND estacao = %s AND data_fechamento = ANY(%s)
                        """, (f"Erro Interno: {str(e_loop)[:200]}", datetime.now(), empresa_erp, estacao, datas))
                        conn.commit()
                    except:
                        pass

    except Exception as e_geral:
        logging.error(f"Erro CRÍTICO no processador de fechamentos: {e_geral}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn and not getattr(conn, 'closed', True):
            if cur: cur.close()
            conn.close()