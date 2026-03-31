import logging

def limpar_dados_antigos(cur, empresas_ativas_erp):
    """
    Remove registros pendentes com mais de 30 dias ou de lojas desativadas/inexistentes.
    """
    try:     
        # Limpeza da fila de vendas
        # Se a venda está na fila há mais de 30 dias e nunca subiu, provavelmente é lixo.
        cur.execute("""
            DELETE FROM int_scanntech_vendas
            WHERE data_hora_inclusao < CURRENT_DATE - INTERVAL '30 days'
               OR empresa NOT IN %s
        """, (tuple(empresas_ativas_erp),))
        
        logging.info("🧹 Faxina concluída com sucesso.")
    except Exception as e:
        logging.error(f"❌ Erro na manutenção do banco: {e}")