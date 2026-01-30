# monitor.pyw

import sys
import os
import time
import threading
from pathlib import Path
from datetime import datetime
from tkinter import messagebox
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from ttkbootstrap.scrolled import ScrolledText

# ==============================================================================
# DEFINIÇÃO DE CONSTANTES
# ==============================================================================

def get_root_dir():
    """Retorna o diretório raiz do projeto"""
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).parent.resolve()
    else:
        return Path(__file__).parent.parent.resolve()

ROOT_DIR = get_root_dir()
LOG_DIR = ROOT_DIR / "logs"
PID_FILE = ROOT_DIR / "integrador.pid"

sys.path.append(str(ROOT_DIR))
from scanntech.config.settings import carregar_configuracoes

# ==============================================================================
# FUNÇÕES DE VERIFICAÇÃO
# ==============================================================================

def is_integrador_really_running():
    """Verifica se o integrador está rodando SEM abrir CMD"""
    try:
        # 1. Verifica se o arquivo PID existe
        if not PID_FILE.exists():
            return False
        
        # 2. Lê o PID
        with open(PID_FILE, 'r') as f:
            pid_str = f.read().strip()
        
        if not pid_str or not pid_str.isdigit():
            return False
        
        pid = int(pid_str)
        
        # 3. Verifica se o processo existe usando ctypes (SEM abrir CMD!)
        import ctypes
        kernel32 = ctypes.windll.kernel32
        
        # PROCESS_QUERY_INFORMATION = 0x0400
        handle = kernel32.OpenProcess(0x0400, False, pid)
        
        if handle:
            kernel32.CloseHandle(handle)
            return True
        
        return False
        
    except Exception as e:
        print(f"Erro ao verificar integrador: {e}")
        return False

# ==============================================================================
# CLASSE PRINCIPAL
# ==============================================================================

class MonitorApp:
    def __init__(self, root):
        self.root = root
        self.is_running = True
        self.integrador_ativo = False
        self.last_log_position = 0
        self._setup_paths()
        self._setup_gui()
        self._start_background_threads()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _setup_paths(self):
        """Define os caminhos para os executáveis"""
        self.configurador_exe = ROOT_DIR / "Configurador.exe"
        self.integrador_exe = ROOT_DIR / "Integrador.exe"

    def _setup_gui(self):
        """Cria a interface gráfica"""
        self.root.title("Monitor do Integrador Scanntech")
        self.centralizar_janela(900, 650)
        
        # Área de texto para logs (SEM state aqui!)
        self.text_area = ScrolledText(self.root, padding=10, font=("Consolas", 9), autohide=True)
        self.text_area.pack(padx=10, pady=10, fill=BOTH, expand=True)
        
        # Configurar tags de cor
        self.text_area.text.tag_configure("info", foreground="#4CAF50")
        self.text_area.text.tag_configure("warning", foreground="#FFC107")
        self.text_area.text.tag_configure("error", foreground="#F44336")
        self.text_area.text.tag_configure("debug", foreground="#2196F3")
        self.text_area.text.tag_configure("initial", foreground="#888888", justify='center')
        
        # Mensagem inicial
        self.text_area.text.insert(END, "\n\n⏳ Iniciando monitor...\n\nVerificando status do integrador...", "initial")
        self.text_area.text.configure(state='disabled')  # Agora sim, DEPOIS de inserir
        
        # Frame para botões
        button_frame = ttk.Frame(self.root)
        button_frame.pack(side=BOTTOM, pady=(0, 10), fill=X, padx=10)
        
        self.btn_config = ttk.Button(button_frame, text="⚙️ Configurações", command=self._open_configurador)
        self.btn_config.pack(side=LEFT, padx=(0, 5))
        
        self.btn_toggle = ttk.Button(button_frame, text="Carregando...", command=self._toggle_integrador, state="disabled")
        self.btn_toggle.pack(side=LEFT, padx=5)
        
        self.btn_logs = ttk.Button(button_frame, text="📄 Abrir Log Completo", command=self._open_log_file)
        self.btn_logs.pack(side=RIGHT)

    def _start_background_threads(self):
        """Inicia threads de segundo plano"""
        LOG_DIR.mkdir(exist_ok=True)
        
        # Thread de status (verifica a cada 3 segundos)
        status_thread = threading.Thread(target=self._status_updater_thread, daemon=True)
        status_thread.start()
        
        # Thread de leitura de log (roda continuamente)
        log_thread = threading.Thread(target=self._tail_log_thread, daemon=True)
        log_thread.start()

    def _status_updater_thread(self):
        """Atualiza o status a cada 3 segundos"""
        while self.is_running:
            self._update_gui_status()
            time.sleep(3)

    def _update_gui_status(self):
        """Atualiza botões baseado no status"""
        if not self.is_running:
            return
        
        try:
            configs = carregar_configuracoes()
            integracao_ativa = configs.get('geral', {}).get("habilitar_integracao_scanntech", "false").lower() == "true"
        except Exception:
            integracao_ativa = False
        
        integrador_rodando = is_integrador_really_running()
        
        # Atualiza botão
        if not integracao_ativa:
            self.btn_toggle.config(text="⏸️ Integração Desativada", bootstyle="secondary", state="disabled")
            if self.integrador_ativo:
                self._show_message("⚠️ Integração desativada nas configurações.\n\nAbra as Configurações para ativá-la.", "initial")
                self.integrador_ativo = False
        
        elif integrador_rodando:
            self.btn_toggle.config(text="⏹️ Parar Integrador", bootstyle="danger", state="normal")
            if not self.integrador_ativo:
                self._show_message(f"{datetime.now():%Y-%m-%d %H:%M:%S} - ✅ Integrador ativo. Exibindo logs em tempo real...\n\n", "info")
                self.integrador_ativo = True
                self.last_log_position = 0
        
        else:
            self.btn_toggle.config(text="▶️ Iniciar Integrador", bootstyle="success", state="normal")
            if self.integrador_ativo:
                self._show_message("⏸️ Integrador inativo.\n\nClique em 'Iniciar Integrador' para começar.", "initial")
                self.integrador_ativo = False

    def _show_message(self, message, tag):
        """Mostra uma mensagem na área de texto"""
        self.text_area.text.configure(state='normal')
        self.text_area.delete('1.0', END)
        self.text_area.insert(END, f"\n\n{message}", tag)
        self.text_area.configure(state='disabled')

    def _tail_log_thread(self):
        """Lê o arquivo de log em tempo real"""
        log_file = LOG_DIR / "integrador.log"
        
        while self.is_running:
            # Só lê log se o integrador estiver rodando
            if not is_integrador_really_running():
                time.sleep(1)
                continue
            
            if not log_file.exists():
                time.sleep(1)
                continue
            
            try:
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    # Vai para a última posição lida
                    f.seek(self.last_log_position)
                    
                    while self.is_running and is_integrador_really_running():
                        lines = f.readlines()
                        
                        if not lines:
                            self.last_log_position = f.tell()
                            time.sleep(0.5)
                            continue
                        
                        # Processar todas as linhas de uma vez
                        self.text_area.text.configure(state='normal')
                        
                        for line in lines:
                            line = line.strip()
                            if line:
                                # Determinar tag baseado no conteúdo
                                level_tag = "info"
                                if "WARNING" in line or "⚠️" in line:
                                    level_tag = "warning"
                                elif "ERROR" in line or "❌" in line:
                                    level_tag = "error"
                                elif "DEBUG" in line:
                                    level_tag = "debug"
                                
                                self.text_area.insert(END, line + '\n', level_tag)
                        
                        self.text_area.see(END)
                        self.text_area.configure(state='disabled')
                        
                        # Atualizar posição
                        self.last_log_position = f.tell()
            
            except Exception as e:
                print(f"Erro ao ler log: {e}")
                time.sleep(2)

    def _toggle_integrador(self):
        """Inicia ou para o integrador"""
        if is_integrador_really_running():
            # Para o integrador
            try:
                with open(PID_FILE, 'r') as f:
                    pid = int(f.read().strip())
                
                # Mata o processo usando ctypes (SEM abrir CMD!)
                import ctypes
                kernel32 = ctypes.windll.kernel32
                
                # PROCESS_TERMINATE = 0x0001
                handle = kernel32.OpenProcess(0x0001, False, pid)
                
                if handle:
                    kernel32.TerminateProcess(handle, 0)
                    kernel32.CloseHandle(handle)
                
                # Remove o arquivo PID
                if PID_FILE.exists():
                    PID_FILE.unlink()
                
                self.text_area.text.configure(state='normal')
                self.text_area.insert(END, f"\n{datetime.now():%Y-%m-%d %H:%M:%S} - ⏹️ Integrador parado pelo usuário.\n", "warning")
                self.text_area.configure(state='disabled')
                
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao parar o integrador: {e}")
        else:
            # Inicia o integrador
            if self.integrador_exe.exists():
                try:
                    import subprocess
                    subprocess.Popen([str(self.integrador_exe)], creationflags=subprocess.CREATE_NO_WINDOW)
                    print("🚀 Integrador iniciado pelo monitor")
                except Exception as e:
                    messagebox.showerror("Erro", f"Falha ao iniciar o integrador: {e}")
            else:
                messagebox.showerror("Erro", "Executável do integrador não encontrado.")
        
        # Atualiza status após 2 segundos
        self.root.after(2000, self._update_gui_status)

    def _open_configurador(self):
        """Abre o configurador"""
        if self.configurador_exe.exists():
            import subprocess
            subprocess.Popen([str(self.configurador_exe)])
        else:
            messagebox.showerror("Erro", "Configurador não encontrado.")

    def _open_log_file(self):
        """Abre o arquivo de log"""
        log_file = LOG_DIR / "integrador.log"
        
        if log_file.exists():
            os.startfile(log_file)
        else:
            messagebox.showwarning("Arquivo não encontrado", "O arquivo de log ainda não foi criado.")

    def centralizar_janela(self, largura, altura):
        """Centraliza a janela na tela"""
        self.root.update_idletasks()
        largura_tela = self.root.winfo_screenwidth()
        altura_tela = self.root.winfo_screenheight()
        x = (largura_tela // 2) - (largura // 2)
        y = (altura_tela // 2) - (altura // 2)
        self.root.geometry(f'{largura}x{altura}+{x}+{y}')

    def _on_close(self):
        """Executado ao fechar a janela"""
        self.is_running = False
        self.root.destroy()

# ==============================================================================
# PONTO DE ENTRADA
# ==============================================================================

if __name__ == '__main__':
    root = ttk.Window(themename="darkly")
    app = MonitorApp(root)
    root.mainloop()
