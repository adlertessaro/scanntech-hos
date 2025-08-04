# forcar_envio_fechamento.py

import sys
import os
from datetime import datetime

# Adiciona o caminho absoluto da pasta raiz do projeto ao sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from config.settings import carregar_configuracoes
from services.payloads.fechamentos_payload import montar_payload_do_fechamento
from api.scanntech_api_fechamentos import enviar_fechamentos_lote

def forcar_envio_fechamento_hoje():
    try:
        config = carregar_configuracoes()
        
        empresa = int(config.get("empresa") or 0)
        hoje = datetime.today().date()

        print(f"🚀 Forçando envio de fechamento CONSOLIDADO para empresa {empresa}, data {hoje}")

        # Chamamos a função SEM o id_caixa, para que ela some os totais de todos os caixas.
        payload = montar_payload_do_fechamento(empresa, config, hoje)

        if not payload:
            print("⚠️ Nenhum dado encontrado para envio na data de hoje.")
            return

        # Para a API, usamos um ID de caixa padrão, como o do 'local'.
        id_caixa_para_api = config.get("local") or "1"
        print(f"ℹ️ Enviando fechamento consolidado para a API sob o id_caixa: {id_caixa_para_api}")

        resultado = enviar_fechamentos_lote(config, id_caixa_para_api, payload)

        print("✅ Resultado da requisição:")
        print(resultado)

    except Exception as e:
        print(f"❌ Ocorreu um erro durante o processo: {e}")


if __name__ == "__main__":
    forcar_envio_fechamento_hoje()