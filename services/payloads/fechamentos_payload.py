import sys
import os
from datetime import datetime
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from db.conexao import conectar

def montar_payload_do_fechamento(empresa, config, data_fechamento, estacao=None):
    """
    Calcula o fechamento com base nos VALORES REAIS ENVIADOS E ACEITOS pela API.
    Usa o campo 'valor_enviado' da tabela int_scanntech_vendas_logs.
    """
    log_avancado = config.get('log_avancado', 'false').lower() == 'true'
    
    if estacao is not None:
        logging.info(f"Montando payload para: Empresa={empresa}, Data={data_fechamento}, Estação={estacao}")
    else:
        logging.info(f"Montando payload CONSOLIDADO para: Empresa={empresa}, Data={data_fechamento}")
    
    conn = None
    try:
        conn = conectar()
        cur = conn.cursor()

        if isinstance(data_fechamento, str):
            # Se vier string 'YYYY-MM-DD', usamos direto
            data_formatada_para_sql = data_fechamento
            # Para o JSON precisamos do objeto date se for usar strftime depois
            # Mas como o payload usa strftime, vamos garantir que seja objeto
            try:
                data_obj = datetime.strptime(data_fechamento, '%Y-%m-%d').date()
                data_fechamento = data_obj # sobrescreve para uso posterior
            except:
                pass 
        else:
            data_formatada_para_sql = data_fechamento.strftime('%Y-%m-%d')
        
        if log_avancado:
            logging.info(f"\n{'='*80}")
            logging.info(f"🔍 LOG AVANÇADO - FECHAMENTO")
            logging.info(f"{'='*80}")
            logging.info(f"   🏢 Empresa: {empresa}")
            logging.info(f"   📅 Data: {data_fechamento}")
            logging.info(f"   🖥️  Estação: {estacao or 'CONSOLIDADO'}")

        sql_base = """
            SELECT 
                tipo_evento,
                ABS(valor_enviado) as valor_final
            FROM int_scanntech_vendas_logs
            WHERE empresa = %s 
            AND data_registro = %s
            AND id_lote IS NOT NULL
        """
        
        params = [empresa, data_formatada_para_sql]
        
        if estacao is not None:
            sql_base += " AND estacao = %s"
            params.append(estacao)
        
        cur.execute(sql_base, params)
        movimentos = cur.fetchall()
        
        if not movimentos:
            logging.info("Sem movimentos ENVIADOS E ACEITOS para os critérios fornecidos.")
            return []
        
        if log_avancado:
            logging.info(f"\n   📊 MOVIMENTOS ENCONTRADOS: {len(movimentos)}")
        
        # Calcular valores
        valor_vendas = 0.0
        valor_cancelamentos = 0.0
        qtd_vendas = 0
        qtd_cancelamentos = 0

        for tipo_evento, valor_final in movimentos:
            valor_float = float(valor_final)
            tipo = str(tipo_evento).strip().upper() # Garante a comparação limpa
            
            # Se for um dos códigos de cancelamento conhecidos
            if tipo in ('CC', 'DV', 'DP'):
                valor_cancelamentos += valor_float
                qtd_cancelamentos += 1
                if log_avancado:
                    logging.info(f"      ❌ CANCEL: R$ {valor_float:.2f}")
            else:
                # Se for qualquer outro (VP, VV, VENDA, etc), conta como VENDA
                valor_vendas += valor_float
                qtd_vendas += 1
                if log_avancado:
                    logging.info(f"      ✓ VENDA ({tipo}): R$ {valor_float:.2f}")

        # montoVentaLiquida para a Scanntech (Líquido)
        valor_liquido = valor_vendas - valor_cancelamentos

        payload = [
            {
                "fechaVentas": data_fechamento.strftime("%Y-%m-%d"),
                "montoVentaLiquida": round(valor_liquido, 2),
                "montoCancelaciones": round(valor_cancelamentos, 2),
                "cantidadMovimientos": int(qtd_vendas),
                "cantidadCancelaciones": int(qtd_cancelamentos),
                # Adicionamos este campo extra apenas para uso interno do nosso processor
                "_montoVentaBruta": round(valor_vendas, 2) 
            }
        ]
        
        if log_avancado:
            logging.info(f"\n   💰 TOTAIS CALCULADOS:")
            logging.info(f"      Vendas (cancelacion=false): {qtd_vendas} movimentos = R$ {valor_vendas:.2f}")
            logging.info(f"      Cancelamentos (cancelacion=true): {qtd_cancelamentos} movimentos = R$ {valor_cancelamentos:.2f}")
            logging.info(f"      Valor Líquido: R$ {valor_liquido:.2f}")
            
            logging.info(f"\n   🎯 PAYLOAD FINAL:")
            logging.info(json.dumps(payload, indent=2, ensure_ascii=False))
            logging.info(f"{'='*80}\n")
        else:
            logging.info(f"Payload gerado: Líquido=R${valor_liquido:.2f}, Vendas={qtd_vendas}, Cancels={qtd_cancelamentos}")
        
        return payload
        
    except Exception as e:
        logging.error(f"❌ ERRO ao montar payload do fechamento: {e}", exc_info=True)
        return []
    
    finally:
        if conn and not getattr(conn, 'closed', True):
            cur.close()
            conn.close()
