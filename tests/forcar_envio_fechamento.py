# tests/forcar_envio_fechamento.py

import sys
import os
from datetime import date, datetime, timedelta
import json
import logging

# Configuração básica do logging para vermos os passos da montagem do payload
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Adiciona o caminho absoluto da pasta raiz do projeto ao sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from config.settings import carregar_configuracoes
from api.scanntech_api_fechamentos import enviar_fechamentos_lote
from services.processors.vendas_processor import limitar_codigo_estacao
# --- ALTERAÇÃO 1: Importamos a função que busca os dados reais ---
from services.payloads.fechamentos_payload import montar_payload_do_fechamento

def _obter_data_alvo() -> date:
    """
    Pede ao usuário para digitar uma data, com validação e um valor padrão (ontem).
    """
    hoje = date.today() - timedelta(days=0)
    hoje_str = hoje.strftime('%d/%m/%Y')
    
    while True:
        prompt = f"\nDigite a data alvo (DD/MM/AAAA) ou pressione Enter para usar a data de ontem ({hoje_str}): "
        data_input = input(prompt).strip()

        if not data_input:
            print(f"Usando data padrão: {hoje_str}")
            return hoje
        
        try:
            data_alvo = datetime.strptime(data_input, '%d/%m/%Y').date()
            return data_alvo
        except ValueError:
            print("❌ Formato de data inválido. Por favor, use o formato DD/MM/AAAA.")

def forcar_envio_fechamento_real():
    """
    Busca dados reais de fechamento no banco para a PRIMEIRA loja configurada,
    monta um plano de envio, pede confirmação e o executa.
    """
    try:
        configs = carregar_configuracoes()
        config_geral = configs.get('geral', {})
        lojas = configs.get('lojas', [])

        if not lojas:
            logging.error("Nenhuma loja encontrada no arquivo de configuração. Abortando.")
            return

        config_loja_selecionada = lojas[0]
        config_completa_loja = {**config_geral, **config_loja_selecionada}
        empresa_erp = config_completa_loja.get('empresa')

        if not empresa_erp:
            logging.error("Código da empresa (ERP) não encontrado. Abortando.")
            return

        # --- ALTERAÇÃO 2: Usa a função para obter a data dinamicamente ---
        data_alvo = _obter_data_alvo()
        
        # PERGUNTA AO USUÁRIO QUAIS ESTAÇÕES TESTAR
        estacoes_input = input("Digite as estações para teste, separadas por vírgula (ex: 1, 80): ")
        try:
            estacoes_alvo = [int(e.strip()) for e in estacoes_input.split(',')]
        except (ValueError, IndexError):
            print("Entrada inválida. Usando estações padrão [1, 80].")
            estacoes_alvo = [1, 88]
        
        print("\n--- Montando Plano de Envio com dados reais do banco ---")
        
        envios_planejados = []
        for estacao in estacoes_alvo:
            logging.info(f"Buscando dados para a Estação {estacao} na data {data_alvo.strftime('%d/%m/%Y')}...")
            
            payload = montar_payload_do_fechamento(
                empresa=empresa_erp,
                config=config_completa_loja,
                data_fechamento=data_alvo,
                estacao=estacao
            )

            if not payload:
                logging.warning(f"Não foram encontrados movimentos para a Estação {estacao}. Pulando.")
                continue

            id_caixa_formatado = limitar_codigo_estacao(estacao)
            envios_planejados.append({
                "estacao": estacao,
                "id_caixa_formatado": id_caixa_formatado,
                "payload": payload
            })

        if not envios_planejados:
            print("\nNenhum dado de fechamento encontrado para as estações e data especificadas. Nenhuma ação será executada.")
            return

        print("\n" + "="*60)
        print("🚨 ATENÇÃO: Revise o plano de envio abaixo.".center(60))
        print("="*60)
        print(f"🎯 Loja Alvo: Empresa ERP {empresa_erp} (Scanntech ID: {config_completa_loja.get('idempresa')})")
        print(f"📅 Data Alvo: {data_alvo.strftime('%d/%m/%Y')}")

        for i, envio in enumerate(envios_planejados):
            print(f"\n--- ENVIO #{i+1} de {len(envios_planejados)} ---")
            print(f"➡️  Estação de Origem (ERP): {envio['estacao']}")
            print(f"➡️  ID Caixa para API: {envio['id_caixa_formatado']}")
            print("➡️  Corpo (Payload) a ser enviado:\n" + json.dumps(envio['payload'], indent=2))
        
        print("\n" + "="*60)
        confirmacao = input("\n❓ O plano acima está correto? Pressione Enter para EXECUTAR ou 'n' para cancelar: ")

        if confirmacao.lower() == 'n':
            print("❌ Operação cancelada pelo usuário.")
            return

        print("\n--- Executando Envios ---")
        for envio in envios_planejados:
            print(f"\n🚀 Enviando para a Estação {envio['estacao']}...")
            try:
                resultado = enviar_fechamentos_lote(
                    config_completa_loja, 
                    envio['id_caixa_formatado'], 
                    envio['payload']
                )
                print(f"✅ Resultado para Estação {envio['estacao']}:")
                print(json.dumps(resultado, indent=2))
            except Exception as e_envio:
                print(f"❌ Erro ao enviar para a Estação {envio['estacao']}: {e_envio}")

    except Exception as e:
        print(f"❌ Ocorreu um erro geral durante o processo: {e}")

if __name__ == "__main__":
    forcar_envio_fechamento_real()
