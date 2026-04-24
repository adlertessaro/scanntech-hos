import threading
import sys
import logging
import subprocess
from pathlib import Path
from PIL import Image
from pystray import Icon, Menu, MenuItem
from scanntech.utils.logger import configurar_logger, get_root_dir

ROOT_DIR = get_root_dir()

from scanntech.core.loop import IntegradorLoop

# Configura o logger para este processo. Essencial para depuração.
configurar_logger()

# CRIAR ARQUIVO PID
PID_FILE = ROOT_DIR / "integrador.pid"

def criar_arquivo_pid():
    """Cria o arquivo PID para o monitor detectar que estamos rodando"""
    try:
        import os
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))
        logging.debug(f"✅ Arquivo PID criado: {PID_FILE}")
    except Exception as e:
        logging.error(f"❌ Erro ao criar arquivo PID: {e}")

def remover_arquivo_pid():
    """Remove o arquivo PID quando o integrador para"""
    try:
        if PID_FILE.exists():
            PID_FILE.unlink()
            logging.debug("✅ Arquivo PID removido")
    except Exception as e:
        logging.error(f"❌ Erro ao remover arquivo PID: {e}")

# Funções para o Menu do Ícone

def abrir_monitor(icon):
    try:
        monitor_exe = ROOT_DIR / "monitor.exe"
        if monitor_exe.exists():
            subprocess.Popen([str(monitor_exe)])
        else:
            monitor_script = ROOT_DIR / "scanntech" / "monitor" / "monitor.pyw"
            if monitor_script.exists():
                subprocess.Popen([sys.executable, str(monitor_script)])
            else:
                logging.error("❌ Executável/script do Monitor não encontrado.")
    except Exception as e:
        logging.error(f"❌ Falha ao tentar abrir o Monitor: {e}")

def abrir_configurador(icon):
    try:
        configurador_exe = ROOT_DIR / "configurador.exe"
        if configurador_exe.exists():
            subprocess.Popen([str(configurador_exe)])
        else:
            configurador_script = ROOT_DIR / "scanntech" / "config" / "configurador.pyw"
            if configurador_script.exists():
                subprocess.Popen([sys.executable, str(configurador_script)])
            else:
                logging.error("❌ Executável/script do Configurador não encontrado.")
    except Exception as e:
        logging.error(f"❌ Falha ao tentar abrir o Configurador: {e}")

def fechar_integrador(icon, item):
    logging.info("Comando de parada recebido. Encerrando...")
    remover_arquivo_pid()
    parar_evento.set()
    icon.stop()

# Configuração e Execução Principal

if __name__ == "__main__":
    parar_evento = threading.Event()
    integrador = IntegradorLoop(parar_evento)
    
    try:
        #criar o PID
        criar_arquivo_pid()
        
        logging.info("🚀 Iniciando thread do loop principal do integrador...")
        loop_thread = threading.Thread(target=integrador.iniciar, daemon=True)
        loop_thread.start()
        
        logging.info("🎨 Carregando imagem para o ícone da bandeja...")
        # Esta linha agora funcionará, pois ROOT_DIR estará correto
        image_path = ROOT_DIR / "img/logo.ico"
        imagem_icone = Image.open(image_path)

        menu = Menu(
            MenuItem('Abrir Monitor', abrir_monitor, default=True),
            MenuItem('Abrir Configurador', abrir_configurador),
            Menu.SEPARATOR,
            MenuItem('Fechar Integrador', fechar_integrador)
        )

        icon = Icon(
            "IntegradorScanntech",
            icon=imagem_icone,
            title="Integrador Scanntech",
            menu=menu
        )

        logging.info("🔔 Executando ícone da bandeja. O programa ficará ativo em segundo plano.")
        icon.run()

    except Exception as e:
        logging.critical(f"🔥 Falha CRÍTICA ao iniciar o ícone da bandeja: {e}", exc_info=True)
        remover_arquivo_pid()
        sys.exit(1)