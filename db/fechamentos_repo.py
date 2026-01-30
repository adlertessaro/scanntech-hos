# scanntech/db/fechamentos_repo.py
from .conexao import conectar
import logging

# Adiciona os imports necessários
from scanntech.api.scanntech_api_reenvio import consultar_solicitacoes_fechamentos
from scanntech.services.processors.fechamentos_processor import enviar_fechamentos_lote


def buscar_fechamentos_pendentes(empresa):
    """Busca todos os fechamentos com tentativas < 5."""
    # (Esta função permanece como está)
    pass # Mantenha sua implementação original aqui

def marcar_fechamentos_para_reenvio(solicitacoes, empresa):
    """Marca fechamentos como pendentes com base na solicitação da API."""
    # (Esta função permanece como está)
    if not solicitacoes:
        return 0
    conn = conectar()
    cur = conn.cursor()
    total_marcado = 0
    for item in solicitacoes:
        data = item.get('fecha')
        caixa = item.get('codigoCaja')
        if not data: continue
        if caixa:
            cur.execute("""
                UPDATE int_scanntech_fechamentos SET tentativas = 0, erro = 'Reenvio solicitado', id_lote = NULL
                WHERE empresa = %s AND estacao = %s AND data_fechamento = %s
            """, (empresa, caixa, data))
        else:
            cur.execute("""
                UPDATE int_scanntech_fechamentos SET tentativas = 0, erro = 'Reenvio solicitado', id_lote = NULL
                WHERE empresa = %s AND data_fechamento = %s
            """, (empresa, data))
        total_marcado += cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return total_marcado


# --- NOVA FUNÇÃO CHAMADA PELO BOTÃO ---
def forcar_envio_fechamentos_com_verificacao(config):
    """
    Orquestra o processo de forçar o envio de fechamentos a partir de um comando manual.
    """
    empresa = config.get("empresa")
    logging.info("Forçando envio de fechamentos com verificação de reenvio...")

    # Etapa 1: Verificar reenvios
    try:
        logging.info("Verificando solicitações de reenvio de fechamentos...")
        resposta_reenvio = consultar_solicitacoes_fechamentos(config)
        if resposta_reenvio.get("status_code") == 200:
            solicitacoes = resposta_reenvio.get("dados", [])
            if solicitacoes:
                total_marcado = marcar_fechamentos_para_reenvio(solicitacoes, empresa)
                logging.info(f"{total_marcado} fechamentos marcados para reenvio.")
    except Exception as e:
        logging.error(f"Erro ao verificar reenvios de fechamentos: {e}")

    # Etapa 2: Enviar todos os pendentes
    fechamentos_pendentes = buscar_fechamentos_pendentes(empresa)
    if not fechamentos_pendentes:
        logging.info("Nenhum fechamento pendente para enviar.")
        return "Nenhum fechamento pendente encontrado para enviar."
        
    logging.info(f"Encontrados {len(fechamentos_pendentes)} fechamentos pendentes. Enviando...")
    enviar_fechamentos_lote(fechamentos_pendentes, config)
    return f"Processo de envio de {len(fechamentos_pendentes)} fechamentos concluído."