import threading
import time
import sys
import os
from datetime import datetime
import pystray
from PIL import Image

# Adiciona o diretório raiz do projeto ao sys.path
# Isso é crucial para que os imports relativos funcionem após o empacotamento
# O diretório raiz do projeto é Scanntech (um nível acima de 'scanntech')
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Importa o loop original
from scanntech.core.loop import iniciar_loop

# --- Funções para executar o configurador e o monitor ---
def run_configurador_exe():
    # Caminho relativo ao executável principal (main_integrador.exe)
    # Assumimos que 'configurador.exe' estará na mesma pasta 'dist'
    script_dir = os.path.dirname(sys.executable) # Diretorio onde main_integrador.exe esta
    configurador_path = os.path.join(script_dir, "configurador.exe")
    print(f"Tentando iniciar Configurador: {configurador_path}")
    if os.path.exists(configurador_path):
        os.startfile(configurador_path)
    else:
        print(f"ERRO: Configurador.exe não encontrado em {configurador_path}")

def run_monitor_exe():
    # Caminho relativo ao executável principal (main_integrador.exe)
    script_dir = os.path.dirname(sys.executable) # Diretorio onde main_integrador.exe esta
    monitor_path = os.path.join(script_dir, "monitor.exe")
    print(f"Tentando iniciar Monitor: {monitor_path}")
    if os.path.exists(monitor_path):
        os.startfile(monitor_path)
    else:
        print(f"ERRO: Monitor.exe não encontrado em {monitor_path}")

# --- Thread para rodar o loop de integração ---
def run_integration_loop():
    try:
        iniciar_loop() # Chama a função iniciar_loop do seu core/loop.py
    except Exception as e:
        print(f"Erro fatal no loop de integração: {e}")
        # Aqui você pode adicionar lógica para parar o ícone da bandeja ou notificar o usuário

# --- Configuração do ícone da bandeja ---
def setup_tray_icon():
    # Crie um ícone simples. Você pode criar um arquivo .ico e incluí-lo
    # Para o exemplo, vamos criar uma imagem em tempo de execução
    image_path = os.path.join(os.path.dirname(sys.executable), "icone.ico")
    if os.path.exists(image_path):
        image = Image.open(image_path)
    else:
        # Fallback para uma imagem simples se o ícone não for encontrado
        image = Image.new('RGB', (64, 64), 'black')
        dc = ImageDraw.Draw(image)
        dc.text((10,10), "S", fill='white')
    dc = ImageDraw.Draw(image)
    dc.text((10,10), "S", fill='white') # Desenha um 'S' no ícone
    # Se você tiver um arquivo .ico, use: Image.open("caminho/para/seu/icone.ico")

    menu = (
        pystray.MenuItem('Abrir Configurador', run_configurador_exe),
        pystray.MenuItem('Abrir Monitor', run_monitor_exe),
        pystray.MenuItem('Sair', lambda icon: icon.stop()) # Para o ícone
    )

    icon = pystray.Icon("integrador_scanntech", image, "Integrador Scanntech", menu)

    # Inicia o loop de integração em uma thread separada
    integration_thread = threading.Thread(target=run_integration_loop)
    integration_thread.daemon = True # Permite que a thread termine quando o programa principal terminar
    integration_thread.start()

    # Roda o ícone da bandeja. Isso é um loop de eventos, então deve ser o último a ser chamado
    icon.run()
    # Quando icon.run() for parado (pelo MenuItem 'Sair'), o programa principal continua aqui
    # e espera a thread de integração terminar se ela ainda estiver rodando.
    integration_thread.join(timeout=10) # Dá um tempo para a thread terminar
    print("Integrador Scanntech encerrado.")


if __name__ == "__main__":
    # Importa ImageDraw para desenhar no ícone. Precisa ser aqui para evitar import circular com PIL.Image
    from PIL import ImageDraw
    setup_tray_icon()