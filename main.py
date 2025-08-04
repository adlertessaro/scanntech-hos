from datetime import datetime
import os
import sys
from config.settings import carregar_configuracoes, exibir_configuracoes
from services.vendas_service import processar_envio_vendas
import time
from core.loop import iniciar_loop

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

log_path = os.path.join(log_dir, f"monitor_{datetime.today().date()}.log")
sys.stdout = open(log_path, "a", encoding="utf-8", buffering=1)  # Redireciona prints

def main():
    config = carregar_configuracoes()
    exibir_configuracoes(config)
    intervalo = int(config.get("Intervalo (s)", 1800))

    print(f"⏱️ Intervalo definido: {intervalo}s")

    while True:
        print("🚀 Enviando vendas pendentes...")
        processar_envio_vendas(config)
        time.sleep(intervalo)

if __name__ == "__main__":
    iniciar_loop()
