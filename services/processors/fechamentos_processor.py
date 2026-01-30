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
        
        # A lógica compara:
        # A) O que consta na tabela de fechamentos (f.valor_enviado_vendas)
        # B) A soma atual dos logs de sucesso (calculo.v_venda_calc)
        
        sql_revalida = """
        UPDATE int_scanntech_fechamentos f
        SET id_lote = NULL,     -- Força o reenvio (remove o ID de sucesso)
            tentativas = 0, 
            erro = NULL,
            data_hora_tentativa = NULL
        FROM (
            SELECT
                l.empresa,
                c.data as data_ref,
                l.estacao,
                -- Soma apenas logs com SUCESSO (id_lote IS NOT NULL)
                SUM(CASE WHEN l.tipo_evento = 'VENDA' THEN l.valor_enviado ELSE 0 END) as v_venda_calc,
                SUM(CASE WHEN l.tipo_evento IN ('CC', 'DV') THEN l.valor_enviado ELSE 0 END) as v_cancel_calc
            FROM int_scanntech_vendas_logs l
            JOIN caixa c ON c.venda = l.venda AND c.empresa = l.empresa
            WHERE l.empresa = %s
              AND l.id_lote IS NOT NULL
              AND c.data >= CURRENT_DATE - 7
            GROUP BY 1, 2, 3
        ) calculo
        WHERE f.empresa = calculo.empresa
          AND f.data_fechamento = calculo.data_ref
          AND f.estacao = calculo.estacao
          AND f.id_lote IS NOT NULL -- Apenas checa os que constam como "Enviados"
          AND (
               -- Se a diferença for maior que 1 centavo, invalida.
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
    CORREÇÃO FINAL: Grava os valores calculados (do payload) no banco após o sucesso,
    garantindo que as colunas 'valor_enviado_vendas' e 'valor_enviado_cancelamentos'
    sejam preenchidas com o que foi efetivamente enviado.
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
        
        for item in lojas_para_processar:
            empresa_erp = item['empresa']
            config_completa_loja = item['config']

            logging.info(f"🔧 Iniciando processador para Empresa {empresa_erp}...")


            # ==============================================================================
            # PASSO FECHAMENTO DE SEGURANÇA
            # Verifica se logs atrasados chegaram e invalida fechamentos antigos
            # ==============================================================================
            invalidar_fechamentos_desatualizados(cur, empresa_erp)
            conn.commit() # Comita a limpeza antes de prosseguir

            # ==============================================================================
            # PASSO 0: POPULAR A TABELA AUTOMATICAMENTE
            # ==============================================================================
            try:
                # Usa CAST(NULL AS TIMESTAMP) para evitar erro de tipo no Postgres
                sql_insert = """
                    INSERT INTO int_scanntech_fechamentos (empresa, data_fechamento, estacao, tentativas, data_hora_tentativa)
                    SELECT DISTINCT 
                        l.empresa, 
                        c.data, 
                        l.estacao, 
                        0, 
                        CAST(NULL AS TIMESTAMP)
                    FROM int_scanntech_vendas_logs l
                    JOIN caixa c ON c.venda = l.venda AND c.empresa = l.empresa
                    WHERE l.empresa = %s
                      AND l.id_lote IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM int_scanntech_fechamentos f
                          WHERE f.empresa = l.empresa 
                            AND f.data_fechamento = c.data 
                            AND f.estacao = l.estacao
                      )
                """
                cur.execute(sql_insert, (empresa_erp,))
                if cur.rowcount > 0:
                    logging.info(f"🆕 Empresa {empresa_erp}: Gerados {cur.rowcount} registros de fechamento pendente.")
                conn.commit()

            except Exception as e:
                logging.error(f"Erro ao inserir pendências: {e}")
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
                        continue

                    # Envia API
                    resposta = enviar_fechamentos_lote(config_completa_loja, estacao_envio, payload_lote)
                    status = resposta.get("status_code")
                    dados = resposta.get("dados", {})

                    if status == 200 and not dados.get("errores"):
                        id_lote = dados.get("idLote", "enviado")
                        logging.info(f"✅ SUCESSO: Lote {id_lote} gravado. Atualizando {len(payload_lote)} registros com valores calculados...")
                        
                        # --- CORREÇÃO IMPORTANTE AQUI ---
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