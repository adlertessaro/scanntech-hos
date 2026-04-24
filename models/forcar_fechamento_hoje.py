# forcar_fechamento_hoje.py

import sys
import os
import logging
from datetime import date, datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from scanntech.config.settings import carregar_configuracoes
from scanntech.db.conexao import conectar
from scanntech.services.payloads.fechamentos_payload import montar_payload_do_fechamento
import scanntech.api.scanntech_api_fechamentos as api_module
from scanntech.services.processors.vendas_processor import limitar_codigo_estacao

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_HOJE = date.today()
# DATA_HOJE = date(2026, 4, 8)

# ============================================================
# 🧪 MOCK — troca o envio real por um simulado
# Para enviar de verdade: mude MODO_TESTE para False
# ============================================================
MODO_TESTE = False  # ATENÇÃO AQUI: se True, o script NÃO enviará dados à API e NÃO alterará o banco, apenas simulará o processo e mostrará os payloads que seriam enviados.

def mock_enviar_fechamentos_lote(config, estacao, payload):
    import json
    logging.info(f"   🧪 [MOCK] Payload que SERIA enviado à API:")
    logging.info(json.dumps(payload, indent=2, ensure_ascii=False))
    return {
        "status_code": 200,
        "dados": {"idLote": "MOCK-TESTE-001"}
    }

if MODO_TESTE:
    import scanntech.api.scanntech_api_fechamentos as api_module
    api_module.enviar_fechamentos_lote = mock_enviar_fechamentos_lote
    logging.warning("⚠️  MODO TESTE ATIVO — nenhum dado será enviado à API.")
# ============================================================


def forcar_fechamento_hoje():
    logging.info(f"🗓️  Forçando fechamento para a data: {DATA_HOJE}")

    configs = carregar_configuracoes()
    config_geral = configs.get('geral', {})
    lojas = configs.get('lojas', [])

    if not lojas:
        logging.warning("⚠️ Nenhuma loja configurada.")
        return

    conn = conectar()
    cur = conn.cursor()

    try:
        for loja in lojas:
            empresa_erp = int(loja['empresa'])
            config_completa = {**config_geral, **loja}

            logging.info(f"\n{'='*60}")
            logging.info(f"🏢 Empresa: {empresa_erp}")

            cur.execute("""
                SELECT DISTINCT estacao
                FROM int_scanntech_vendas_logs
                WHERE empresa = %s
                  AND data_registro = %s
                  AND id_lote IS NOT NULL
            """, (empresa_erp, DATA_HOJE))

            estacoes = list(set([row[0] for row in cur.fetchall()]))

            if not estacoes:
                logging.warning(f"   ⚠️ Nenhuma venda enviada/aceita hoje para Empresa {empresa_erp}.")
                continue

            logging.info(f"   📡 Estações com movimento hoje: {estacoes}")

            for estacao in estacoes:
                try:
                    estacao_envio = limitar_codigo_estacao(estacao)
                    logging.info(f"   🚀 Processando Estação {estacao_envio}...")

                    payload_lote = montar_payload_do_fechamento(
                        empresa_erp, config_completa, DATA_HOJE, estacao
                    )

                    if not payload_lote:
                        logging.warning(f"   ⚠️ Sem dados para montar payload. Estação {estacao}.")
                        continue

                    resposta = api_module.enviar_fechamentos_lote(config_completa, estacao_envio, payload_lote)
                    status = resposta.get("status_code")
                    dados = resposta.get("dados", {})

                    if status == 200 and not dados.get("errores"):
                        id_lote = dados.get("idLote", "enviado")
                        logging.info(f"   ✅ SUCESSO! Lote: {id_lote}")

                        if not MODO_TESTE:
                            for item_payload in payload_lote:
                                cur.execute("""
                                    INSERT INTO int_scanntech_fechamentos
                                        (empresa, data_fechamento, estacao, tentativas,
                                         id_lote, data_hora_tentativa,
                                         valor_enviado_vendas, valor_enviado_cancelamentos, erro)
                                    VALUES (%s, %s, %s, 0, %s, %s, %s, %s, NULL)
                                    ON CONFLICT (empresa, data_fechamento, estacao)
                                    DO UPDATE SET
                                        id_lote = EXCLUDED.id_lote,
                                        data_hora_tentativa = EXCLUDED.data_hora_tentativa,
                                        valor_enviado_vendas = EXCLUDED.valor_enviado_vendas,
                                        valor_enviado_cancelamentos = EXCLUDED.valor_enviado_cancelamentos,
                                        erro = NULL
                                """, (
                                    empresa_erp,
                                    item_payload['fechaVentas'],
                                    estacao,
                                    id_lote,
                                    datetime.now(),
                                    item_payload['montoVentaLiquida'],
                                    item_payload['montoCancelaciones']
                                ))
                            conn.commit()
                            logging.info(f"   💾 Fechamento gravado no banco.")
                        else:
                            logging.info(f"   🧪 [MOCK] Banco NÃO foi alterado.")

                    else:
                        erro_msg = resposta.get("mensagem", str(dados.get("errores", "Erro desconhecido")))
                        logging.error(f"   ❌ FALHA na API: {erro_msg}")

                except Exception as e_estacao:
                    logging.error(f"   ❌ Erro na Estação {estacao}: {e_estacao}", exc_info=True)
                    conn.rollback()

    except Exception as e:
        logging.error(f"❌ Erro crítico: {e}", exc_info=True)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
        logging.info("\n✅ Script finalizado.")


if __name__ == "__main__":
    forcar_fechamento_hoje()