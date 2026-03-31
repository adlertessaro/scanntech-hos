# scanntech/services/processors/vendas_processor.py
"""
Orquestrador principal do processamento de vendas.

Responsabilidades deste arquivo:
  - Carregar configurações e conexão com o banco
  - Iterar lojas e estações
  - Delegar filtragem de vendas, montagem de lotes e envio para submódulos

Submódulos:
  vendas_utils.py        → helpers puros (sem DB)
  vendas_db_helpers.py   → operações de leitura/escrita no banco
  vendas_lote_builder.py → validações + montagem do lote
  vendas_lote_sender.py  → envio à API + tratamento do retorno
"""

import logging
import time

from scanntech.db.conexao import conectar
from scanntech.config.settings import carregar_configuracoes
from scanntech.services.processors.vendas_utils import (
    limitar_codigo_estacao,
    resolver_data_inicio,
)
from scanntech.services.processors.vendas_db_helpers import excluir_venda_da_fila
from scanntech.services.processors.vendas_lote_builder import construir_lote
from scanntech.services.processors.vendas_lote_sender import enviar_grupos


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

        data_inicio = resolver_data_inicio(config_geral)

        conn = conectar()
        cur = conn.cursor()

        for loja_config in lojas:
            try:
                empresa_erp = int(loja_config['empresa'])
                id_empresa_scanntech = loja_config['idempresa']
                id_local_scanntech = loja_config['idlocal']
            except KeyError as e:
                logging.error(
                    f"❌ Configuração incompleta para a loja com ERP ID "
                    f"{loja_config.get('empresa', 'N/A')}. Chave ausente: {e}. Pulando."
                )
                continue

            config_completa_loja = {**config_geral, **loja_config}

            logging.info(
                f"📋 Config da loja {empresa_erp}: "
                f"url1={config_completa_loja.get('url1', 'AUSENTE')[:30]}... "
                f"usuario={config_completa_loja.get('usuario', 'AUSENTE')}"
            )
            logging.info(
                f"\n--- Iniciando processamento de vendas para a Empresa ERP: {empresa_erp} "
                f"(Scanntech ID: {id_empresa_scanntech}) ---"
            )

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
                        cur.execute("""
                            SELECT venda FROM int_scanntech_vendas
                            WHERE empresa = %s AND estacao = %s AND tentativas < 3
                            ORDER BY data_hora_inclusao LIMIT 350
                        """, (empresa_erp, estacao_original))
                        vendas_raw = [int(row[0]) for row in cur.fetchall()]

                        if not vendas_raw:
                            logging.info(f"✅ Fim do processamento para a Estação {estacao_limitada}.")
                            break

                        # ── Filtrar por data de início ──────────────────────
                        vendas = _filtrar_por_data(
                            cur, conn, vendas_raw, empresa_erp, estacao_original, estacao_limitada, data_inicio
                        )
                        if vendas is None:
                            break  # todas anteriores à data_inicio

                        logging.info(f"🧾 Lote selecionado com {len(vendas)} transações.")

                        # ── Construir lote (validações + payloads) ──────────
                        payloads, vendas_enviadas = construir_lote(
                            cur, conn, vendas,
                            empresa_erp, estacao_original, estacao_limitada,
                            config_completa_loja, data_inicio,
                        )

                        if not payloads:
                            logging.info("⚠️  Nenhum payload válido para enviar neste lote.")
                            continue

                        # ── Enviar lote e processar retorno ─────────────────
                        enviar_grupos(
                            cur, conn,
                            vendas_enviadas,
                            config_completa_loja,
                            id_empresa_scanntech,
                            id_local_scanntech,
                            estacao_limitada,
                            empresa_erp,
                            estacao_original,
                        )

                        break

                    except Exception as e_estacao:
                        logging.error(f"❌ Erro ao processar a estação {estacao_limitada}: {e_estacao}")
                        conn.rollback()
                        break  # sai do while para não travar em loop infinito

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


# ──────────────────────────────────────────────────────────────────────────────
# Helper local (só usado pelo orquestrador)
# ──────────────────────────────────────────────────────────────────────────────

def _filtrar_por_data(cur, conn, vendas_raw, empresa_erp, estacao_original, estacao_limitada, data_inicio):
    """
    Remove da fila (e da lista) as vendas anteriores à data_inicio.
    Retorna a lista filtrada, ou None se todas foram removidas.
    """
    if not data_inicio:
        return vendas_raw

    vendas = []
    for venda in vendas_raw:
        cur.execute("SELECT data FROM caixa WHERE venda = %s AND empresa = %s", (venda, empresa_erp))
        row = cur.fetchone()
        if row:
            data_venda = row[0]
            if data_venda >= data_inicio:
                vendas.append(venda)
            else:
                logging.info(f"⏭️  Removendo venda {venda} da fila (data {data_venda} < {data_inicio})")
                excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                conn.commit()

    if not vendas:
        logging.info(
            f"⏭️  Todas as vendas da estação {estacao_limitada} são anteriores a "
            f"{data_inicio.strftime('%d/%m/%Y')}. Pulando."
        )
        return None

    logging.info(
        f"📅 Após filtro de data: {len(vendas)} vendas de {len(vendas_raw)} "
        f"(>= {data_inicio.strftime('%d/%m/%Y')})"
    )
    return vendas