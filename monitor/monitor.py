import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from ttkbootstrap.scrolled import ScrolledText
import os
from datetime import datetime
import glob
import time
import re
import threading

# Função para determinar o nível de log a partir da mensagem
def get_log_level(message):
    if "✅" in message or "🧪" in message or "🗑️" in message:
        return "INFO"
    elif "⚠️" in message:
        return "WARNING"
    elif "❌" in message:
        return "ERROR"
    return "INFO"  # Padrão

# Função para ler o log em tempo real
def tail_log_file(log_file, text_widget, root):
    if not os.path.exists(log_file):
        text_widget.configure(state='normal')
        text_widget.insert(END, f"{datetime.now():%Y-%m-%d %H:%M:%S} - 📁 Aguardando criação do arquivo {log_file}...\n", "info")
        text_widget.configure(state='disabled')
        text_widget.update_idletasks()
        root.update()

    with open(log_file, 'r', encoding='utf-8') as f:
        f.seek(0, os.SEEK_END)  # Ir para o final do arquivo
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.1)  # Esperar novas linhas
                continue
            line = line.strip()
            if line:
                level = get_log_level(line)
                tag = "info" if level == "INFO" else "warning" if level == "WARNING" else "error"
                text_widget.configure(state='normal')
                text_widget.insert(END, line + '\n', tag)
                text_widget.see(END)
                text_widget.configure(state='disabled')
                text_widget.update_idletasks()
                root.update()
                print(f"📺 Exibindo no painel: {line}")  # Depuração

# Configuração do painel visual
def criar_painel():
    root = ttk.Window(themename="darkly")
    root.title("Monitor de Promoções Scanntech")
    root.geometry("600x400")
    text_area = ScrolledText(root, padding=10, font=("Arial", 12))
    text_area.pack(padx=10, pady=10, fill=BOTH, expand=True)
    # Configurar tags para cores
    text_area.text.tag_configure("info", foreground="lightgreen")
    text_area.text.tag_configure("warning", foreground="yellow")
    text_area.text.tag_configure("error", foreground="red")
    return root, text_area

# Função para excluir logs antigos
def delete_old_logs(log_dir="logs"):
    today = datetime.now().strftime("%Y-%m-%d")
    log_pattern = os.path.join(log_dir, "monitor_*.log")
    for log_file in glob.glob(log_pattern):
        file_name = os.path.basename(log_file)
        if file_name.startswith("monitor_") and file_name.endswith(".log"):
            file_date = file_name[8:-4]
            if file_date != today:
                try:
                    os.remove(log_file)
                    print(f"🗑️ Arquivo de log antigo excluído: {log_file}")
                except Exception as e:
                    print(f"⚠️ Falha ao excluir log antigo {log_file}: {e}")

# Função para iniciar o monitor
def iniciar_monitor():
    root, text_area = criar_painel()
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"monitor_{today}.log")

    # Excluir logs antigos
    delete_old_logs(log_dir)

    # Iniciar leitura do log em uma thread separada
    def start_tail():
        tail_log_file(log_file, text_area.text, root)

    thread = threading.Thread(target=start_tail, daemon=True)
    thread.start()

    # Mensagem inicial no painel
    text_area.text.configure(state='normal')
    text_area.text.insert(END, f"{datetime.now():%Y-%m-%d %H:%M:%S} - ✅ Monitor iniciado com sucesso.\n", "info")
    text_area.text.insert(END, f"{datetime.now():%Y-%m-%d %H:%M:%S} - 🧪 Teste de mensagem INFO\n", "info")
    text_area.text.insert(END, f"{datetime.now():%Y-%m-%d %H:%M:%S} - ⚠️ Teste de mensagem WARNING\n", "warning")
    text_area.text.insert(END, f"{datetime.now():%Y-%m-%d %H:%M:%S} - ❌ Teste de mensagem ERROR\n", "error")
    text_area.text.configure(state='disabled')
    text_area.text.see(END)

    return root

if __name__ == '__main__':
    root = iniciar_monitor()
    root.mainloop()