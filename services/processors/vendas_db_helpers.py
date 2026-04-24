"""
Funções auxiliares de acesso ao banco de dados para o processador de vendas.
"""

import logging
from datetime import datetime


def verificar_venda_ja_processada(cur, venda, empresa, tipo_evento):
    """Verifica se o ID de venda já foi processado com sucesso para o tipo de evento."""
    try:
        cur.execute("""
            SELECT COUNT(*) FROM int_scanntech_vendas_logs
            WHERE venda = %s AND empresa = %s AND tipo_evento = %s AND id_lote IS NOT NULL
        """, (venda, empresa, tipo_evento))
        return cur.fetchone()[0] > 0
    except Exception as e:
        logging.error(f"❌ Erro ao verificar evento '{tipo_evento}' da venda {venda}: {e}")
        return False


def verificar_duplicata_por_cupom(cur, venda, empresa, cupom, valor, lancamen, operador, estacao):
    """
    Segunda camada de proteção contra duplicatas.

    Verifica se já existe no log uma venda diferente com o mesmo conjunto de atributos:
    cupom + valor + lancamen + operador + estacao.

    Isso evita que registros com IDs distintos mas dados idênticos sejam enviados
    em duplicidade para a mesma empresa.

    Retorna True se houver duplicata (deve pular o envio), False caso contrário.
    """
    try:
        cur.execute("""
            SELECT l.venda
            FROM int_scanntech_vendas_logs l
            JOIN caixa c ON c.venda = l.venda AND c.empresa = l.empresa
            WHERE l.empresa    = %s
              AND l.venda     != %s
              AND c.cupom      = %s
              AND c.valor      = %s
              AND c.lancamen   = %s
              AND c.operador   = %s
              AND c.estacao    = %s
            LIMIT 1
        """, (empresa, venda, cupom, valor, lancamen, operador, estacao))
        row = cur.fetchone()
        if row:
            logging.info(
                f"⏭️  Duplicata detectada: Venda {venda} possui mesmos "
                f"cupom/valor/lancamen/operador/estacao da venda {row[0]} já enviada. "
                f"Removendo da fila."
            )
            return True
        return False
    except Exception as e:
        logging.error(f"❌ Erro ao verificar duplicata por cupom para venda {venda}: {e}")
        return False


def excluir_venda_da_fila(cur, venda, empresa, estacao):
    try:
        cur.execute("""
            DELETE FROM int_scanntech_vendas
            WHERE venda = %s AND empresa = %s AND estacao = %s
        """, (venda, empresa, estacao))
        return True
    except Exception as e:
        logging.error(f"❌ Erro ao executar DELETE para a venda {venda}: {e}")
        return False


def inserir_log_de_sucesso(cur, venda, empresa, estacao, id_lote, tipo_evento, valor_enviado=None, data_registro=None):
    try:
        cur.execute("""
            INSERT INTO int_scanntech_vendas_logs
            (venda, empresa, estacao, data_registro, data_hora_retorno, id_lote, tipo_evento, valor_enviado)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (venda, empresa, tipo_evento) DO UPDATE SET
            data_hora_retorno = EXCLUDED.data_hora_retorno,
            id_lote           = EXCLUDED.id_lote,
            estacao           = EXCLUDED.estacao,
            valor_enviado     = EXCLUDED.valor_enviado,
            data_registro     = EXCLUDED.data_registro
        """, (venda, empresa, estacao, data_registro, datetime.now(), id_lote, tipo_evento, valor_enviado))
        return True
    except Exception as e:
        logging.error(f"❌ Erro ao logar sucesso: {e}")
        return False


def incrementar_tentativa(cur, venda, empresa, estacao, erro_msg):
    cur.execute("""
        UPDATE int_scanntech_vendas
        SET tentativas = tentativas + 1,
            erro = %s,
            data_hora_tentativa = %s
        WHERE venda = %s AND empresa = %s AND estacao = %s
    """, (str(erro_msg)[:255], datetime.now(), venda, empresa, estacao))