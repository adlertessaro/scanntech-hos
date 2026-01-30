# scanntech/db/vendas_repo.py
from .conexao import conectar
import logging
from scanntech.api.scanntech_api_reenvio import consultar_solicitacoes_vendas
from scanntech.services.processors.vendas_processor import processar_envio_vendas

def buscar_vendas_pendentes(empresa):
    """Busca todas as vendas com tentativas < 5."""
    # (Esta função permanece como está)
    pass # Mantenha sua implementação original aqui

def marcar_vendas_para_reenvio(solicitacoes, empresa):
    """Marca vendas como pendentes com base na solicitação da API."""
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
                UPDATE int_scanntech_vendas SET tentativas = 0, erro = 'Reenvio solicitado'
                WHERE empresa = %s AND estacao = %s AND DATE(data_hora_inclusao) = %s
            """, (empresa, caixa, data))
        else:
            cur.execute("""
                UPDATE int_scanntech_vendas SET tentativas = 0, erro = 'Reenvio solicitado'
                WHERE empresa = %s AND DATE(data_hora_inclusao) = %s
            """, (empresa, data))
        total_marcado += cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return total_marcado

# --- NOVA FUNÇÃO CHAMADA PELO BOTÃO ---
def forcar_envio_vendas_com_verificacao(config):
    """
    Orquestra o processo de forçar o envio de vendas a partir de um comando manual.
    1. Chama a API para verificar solicitações de reenvio.
    2. Marca as vendas solicitadas no banco.
    3. Busca TODAS as vendas pendentes.
    4. Envia os lotes para o processador.
    """
    empresa = config.get("empresa")
    logging.info("Forçando envio de vendas com verificação de reenvio...")

    # Etapa 1: Verificar reenvios
    try:
        logging.info("Verificando solicitações de reenvio de vendas...")
        resposta_reenvio = consultar_solicitacoes_vendas(config)
        if resposta_reenvio.get("status_code") == 200:
            solicitacoes = resposta_reenvio.get("dados", [])
            if solicitacoes:
                total_marcado = marcar_vendas_para_reenvio(solicitacoes, empresa)
                logging.info(f"{total_marcado} vendas marcadas para reenvio.")
    except Exception as e:
        logging.error(f"Erro ao verificar reenvios de vendas: {e}")
        # Continua mesmo se a verificação falhar, para enviar os pendentes

    # Etapa 2: Enviar todos os pendentes
    vendas_pendentes = buscar_vendas_pendentes(empresa)
    if not vendas_pendentes:
        logging.info("Nenhuma venda pendente para enviar.")
        return "Nenhuma venda pendente encontrada para enviar."
    
    logging.info(f"Encontradas {len(vendas_pendentes)} vendas pendentes. Enviando...")
    processar_lote_vendas(vendas_pendentes, config)
    return f"Processo de envio de {len(vendas_pendentes)} vendas concluído."