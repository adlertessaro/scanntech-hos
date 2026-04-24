# scanntech/services/processors/vendas_lote_sender.py
"""
Responsável por enviar os lotes (VV e DV) à API da Scanntech e processar
o retorno (sucesso, erros por cupom, falha HTTP).
"""

import json
import logging
import time
from datetime import datetime

from scanntech.api.scanntech_api_vendas import enviar_vendas_lote
from scanntech.services.processors.vendas_db_helpers import (
    excluir_venda_da_fila,
    inserir_log_de_sucesso,
)


def enviar_grupos(
    cur, conn,
    vendas_enviadas,
    config_completa_loja,
    id_empresa_scanntech,
    id_local_scanntech,
    estacao_limitada,
    empresa_erp,
    estacao_original,
):
    """
    Separa vendas_enviadas em grupo VV (vendas) e DV (cancelamentos),
    envia cada grupo como lote e trata o retorno da API.
    """
    grupo_vv = {v: i for v, i in vendas_enviadas.items() if not i['payload'].get('cancelacion')}
    grupo_dv = {v: i for v, i in vendas_enviadas.items() if i['payload'].get('cancelacion')}

    for nome_grupo, grupo in [('VENDAS', grupo_vv), ('CANCELAMENTOS', grupo_dv)]:
        if not grupo:
            continue

        lote_payloads = [i['payload'] for i in grupo.values()]
        logging.info(f"🚀 Enviando lote de {len(lote_payloads)} {nome_grupo} para Estação {estacao_limitada}...")

        time.sleep(0.3)

        resposta_lote = enviar_vendas_lote(
            config_completa_loja,
            id_empresa_scanntech,
            id_local_scanntech,
            estacao_limitada,
            lote_payloads,
        )

        status = resposta_lote.get("status_code")
        dados = resposta_lote.get("dados", {})

        if status == 200:
            _processar_retorno_200(
                cur, conn, grupo, nome_grupo,
                dados, empresa_erp, estacao_original,
            )
        else:
            _processar_retorno_erro(
                cur, conn, grupo, nome_grupo,
                resposta_lote, status, empresa_erp,
            )


# ──────────────────────────────────────────────────────────────────────────────
# Helpers internos
# ──────────────────────────────────────────────────────────────────────────────

def _processar_retorno_200(cur, conn, grupo, nome_grupo, dados, empresa_erp, estacao_original):
    id_lote = dados.get("idLote", "desconhecido")
    erros_api = dados.get("errores", [])
    logging.info(f"✅ Lote {nome_grupo} enviado (ID: {id_lote}). {len(erros_api)} erro(s).")

    try:
        vendas_com_erro_api = _mapear_erros_api(grupo, erros_api)

        # Sucesso
        vendas_com_sucesso = 0
        for venda, info in grupo.items():
            if venda not in vendas_com_erro_api:
                inserir_log_de_sucesso(
                    cur, venda, empresa_erp, estacao_original,
                    id_lote, info['tipo_evento'],
                    info['payload']['total'],
                    data_registro=info['data_venda'],
                )
                excluir_venda_da_fila(cur, venda, empresa_erp, estacao_original)
                vendas_com_sucesso += 1

        # Erro retornado pela API por cupom
        for venda_id, error_msg in vendas_com_erro_api.items():
            cur.execute("""
                UPDATE int_scanntech_vendas
                SET tentativas = tentativas + 1,
                    erro = %s,
                    data_hora_tentativa = %s
                WHERE venda = %s AND empresa = %s AND tentativas < 3
            """, (error_msg[:255], datetime.now(), venda_id, empresa_erp))

        conn.commit()

        logging.info(f"")
        logging.info(f"📊 RESUMO DO LOTE {nome_grupo} (ID: {id_lote}):")
        logging.info(f"   ✅ Vendas aceitas: {vendas_com_sucesso}")
        logging.info(f"   ❌ Vendas rejeitadas: {len(vendas_com_erro_api)}")
        logging.info(f"   📦 Total no lote: {len(grupo)}")

        if vendas_com_erro_api:
            logging.warning(f"")
            logging.warning(f"⚠️ ATENÇÃO: {len(vendas_com_erro_api)} venda(s) permanecem na fila com erro registrado.")
            logging.warning(f"   Verifique os logs acima para detalhes dos erros.")

    except Exception as db_error:
        logging.error(f"❌ Erro ao atualizar o banco após retorno da API: {db_error}")
        conn.rollback()


def _processar_retorno_erro(cur, conn, grupo, nome_grupo, resposta_lote, status, empresa_erp):
    erro_http = resposta_lote.get("mensagem", f"Erro HTTP {status}")
    logging.error(f"❌ Falha no envio do lote {nome_grupo}: {erro_http}")

    try:
        for venda in grupo:
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


def _mapear_erros_api(grupo, erros_api):
    """Mapeia os erros retornados pela API (por número de cupom) ao ID interno da venda."""
    vendas_com_erro = {}

    for erro in erros_api:
        numero_rejeitado = str(erro.get("numero", ""))
        venda_real = None

        for venda_id, info in grupo.items():
            if str(info['payload'].get('numero')) == numero_rejeitado:
                venda_real = venda_id
                break

        if venda_real:
            error_obj = erro.get("error", {})
            error_code = error_obj.get("code", "ERRO_DESCONHECIDO")
            error_message = error_obj.get("message", "Sem mensagem")
            error_full = f"{error_code}: {error_message}"
            vendas_com_erro[venda_real] = error_full

            logging.error(f"❌ Venda {venda_real} (Cupom {numero_rejeitado}) REJEITADA:")
            logging.error(f"   Código: {error_code}")
            logging.error(f"   Mensagem: {error_message}")

            if error_code in ['FALLO_MOV_SUMA_PAGOS', 'FALLO_MOV_IMPORTE_DETALLES']:
                logging.error(f"   📄 PAYLOAD COMPLETO:")
                logging.error(json.dumps(info['payload'], indent=2, ensure_ascii=False))
        else:
            logging.error(f"⚠️ API rejeitou o cupom {numero_rejeitado}, mas o ID da venda não foi localizado!")

    return vendas_com_erro