# configurador.py

import tkinter as tk
from tkinter import messagebox
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from ttkbootstrap.tooltip import ToolTip
from datetime import datetime
import os
import sys
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# --- INÍCIO DA ALTERAÇÃO ---

# Importa a função de carregar e a variável CONFIG_PATH do módulo de configurações.
# Agora, este arquivo usará EXATAMENTE o mesmo caminho que o settings.py.
from scanntech.config.settings import carregar_configuracoes, CONFIG_PATH
from scanntech.models.gerar_fechamentos_pendentes import gerar_fechamentos_pendentes

# --- FIM DA ALTERAÇÃO ---

from scanntech.config.setup_db import criar_tabelas_scanntech

sys.path.append(str(Path(__file__).parent.parent.parent))
import json
import logging

# Configurar logging para depuração
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# --- O BLOCO DE CÓDIGO INCORRETO FOI REMOVIDO DAQUI ---
# A definição de CONFIG_PATH agora é importada, garantindo consistência.

def validar_config_hos():
    caminho = Path.home() / ".hos" / "config"
    if not caminho.exists():
        messagebox.showerror("Erro", "Arquivo de configuração do HOS Farma não foi encontrado.\nO configurador será encerrado.")
        return False

    try:
        with open(caminho, "r", encoding="utf-8") as f:
            dados = json.load(f)
            banco = dados.get("Configuracoes", {}).get("BANCO_DADOS", "").upper()
            if banco != "POSTGRES":
                messagebox.showerror("Banco não suportado", "Somente disponível para POSTGRES.\nO configurador será encerrado.")
                return False
    except Exception as e:
        messagebox.showerror("Erro", f"Falha ao ler o arquivo de configuração:\n{str(e)}")
        return False

    return True

class ConfiguradorApp:
    def atualizar_botoes_geral(self, *args):
        """Atualiza o estado dos botões na aba Geral com base na integração."""
        estado = "normal" if self.campos["Habilitar Integração Scanntech"].get() else "disabled"
        for widget in self.frame_geral.winfo_children():
            if isinstance(widget, ttk.Button):
                widget.configure(state=estado)

    def __init__(self, master):
        self.master = master
        master.title("Configurador Scanntech")
        self.centralizar_janela(master, 560, 700)

        self.style = ttk.Style(theme="lumen")
        self.notebook = ttk.Notebook(master)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)

        self.frame_config = ttk.Frame(self.notebook)
        self.notebook.add(self.frame_config, text="Configurações")

        canvas = tk.Canvas(self.frame_config)
        scrollbar = ttk.Scrollbar(self.frame_config, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.frame_geral = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.frame_geral, text="Geral")

        self.campos = {
            "Habilitar Integração Scanntech": tk.BooleanVar(value=True),
            "Usuário": tk.StringVar(),
            "Senha": tk.StringVar(),
            "URL 1": tk.StringVar(),
            "URL 2": tk.StringVar(),
            "URL 3": tk.StringVar(),
            "Empresa": tk.StringVar(),
            "Local": tk.StringVar(),
            "CRM": tk.StringVar(),
            "Código da empresa": tk.StringVar(),
            "Intervalo (s)": tk.StringVar(value="1800"),
            "Data de Início": tk.StringVar(value=datetime.now().strftime("%d/%m/%Y")),
            "Data início envio de vendas": tk.StringVar(value=datetime.now().strftime("%d/%m/%Y")),
            "Data início envio de fechamentos": tk.StringVar(value=datetime.now().strftime("%d/%m/%Y"))
        }

        vcmd = (self.master.register(self.somente_numeros), '%P')

        integracao_frame = ttk.LabelFrame(scrollable_frame, text="Integração", padding=10)
        integracao_frame.pack(fill='x', padx=5, pady=5)
        check_integracao = ttk.Checkbutton(
            integracao_frame,
            text="Habilitar Integração Scanntech",
            variable=self.campos["Habilitar Integração Scanntech"],
            bootstyle="round-toggle"
        )
        check_integracao.pack(anchor="w", pady=5)
        ToolTip(
            check_integracao,
            text="Ativa ou desativa a integração com a Scanntech. Quando desativada, as ações de envio são ignoradas.",
            bootstyle="inverse"
        )

        credenciais_frame = ttk.LabelFrame(scrollable_frame, text="Credenciais", padding=10)
        credenciais_frame.pack(fill='x', padx=5, pady=5)
        for label in ["Usuário", "Senha", "URL 1", "URL 2", "URL 3"]:
            row_index = ["Usuário", "Senha", "URL 1", "URL 2", "URL 3"].index(label)
            ttk.Label(credenciais_frame, text=label).grid(row=row_index, column=0, sticky="e", padx=5, pady=5)
            entry = ttk.Entry(
                credenciais_frame,
                textvariable=self.campos[label],
                width=30,
                bootstyle="secondary"
            )
            if label == "Senha":
                entry.config(show="*")
                self.entry_senha = entry
            entry.grid(row=row_index, column=1, sticky="w", padx=5, pady=5)
            if label.startswith("URL"):
                ToolTip(entry, text=f"Endereço da API Scanntech ({label})", bootstyle="inverse")

        empresa_frame = ttk.LabelFrame(scrollable_frame, text="Empresa", padding=10)
        empresa_frame.pack(fill='x', padx=5, pady=5)
        
        labels = ["Empresa", "Local", "CRM", "Código da empresa", "Intervalo (s)"]
        for label in labels:
            row_index = labels.index(label)
            ttk.Label(empresa_frame, text=label).grid(row=row_index, column=0, sticky="e", padx=5, pady=5)
            entry = ttk.Entry(
                empresa_frame,
                textvariable=self.campos[label],
                width=30,
                bootstyle="secondary",
                validate="key" if label in ["CRM", "Código da empresa", "Empresa", "Local", "Intervalo (s)"] else None,
                validatecommand=vcmd if label in ["CRM", "Código da empresa", "Empresa", "Local", "Intervalo (s)"] else None
            )
            entry.grid(row=row_index, column=1, sticky="w", padx=5, pady=5)
            if label == "Intervalo (s)":
                ToolTip(entry, text="Intervalo em segundos entre execuções do integrador", bootstyle="inverse")

        row_index = len(labels)
        
        ttk.Label(empresa_frame, text="Data de Início").grid(row=row_index, column=0, sticky="e", padx=5, pady=5)
        self.data_entry = ttk.Entry(empresa_frame, textvariable=self.campos["Data de Início"], width=30, bootstyle="secondary")
        self.data_entry.grid(row=row_index, column=1, sticky="w", padx=5, pady=5)
        self.data_entry.bind("<Key>", self.formatar_data)
        ToolTip(self.data_entry, text="Data inicial para processamento de dados (formato: DD/MM/AAAA)", bootstyle="inverse")

        row_index += 1
        ttk.Label(empresa_frame, text="Data início envio de vendas").grid(row=row_index, column=0, sticky="e", padx=5, pady=5)
        self.data_entry_vendas = ttk.Entry(empresa_frame, textvariable=self.campos["Data início envio de vendas"], width=30, bootstyle="secondary")
        self.data_entry_vendas.grid(row=row_index, column=1, sticky="w", padx=5, pady=5)
        self.data_entry_vendas.bind("<Key>", self.formatar_data)
        ToolTip(self.data_entry_vendas, text="Data inicial para envio de vendas (formato: DD/MM/AAAA)", bootstyle="inverse")

        row_index += 1
        ttk.Label(empresa_frame, text="Data início envio de fechamentos").grid(row=row_index, column=0, sticky="e", padx=5, pady=5)
        self.data_entry_fechamentos = ttk.Entry(empresa_frame, textvariable=self.campos["Data início envio de fechamentos"], width=30, bootstyle="secondary")
        self.data_entry_fechamentos.grid(row=row_index, column=1, sticky="w", padx=5, pady=5)
        self.data_entry_fechamentos.bind("<Key>", self.formatar_data)
        ToolTip(self.data_entry_fechamentos, text="Data inicial para envio de fechamentos (formato: DD/MM/AAAA)", bootstyle="inverse")

        btn_frame = ttk.Frame(scrollable_frame)
        btn_frame.pack(fill='x', pady=20)
        ttk.Button(btn_frame, text="Salvar", command=self.salvar_config, bootstyle="success").pack(side="left", padx=10)
        ttk.Button(btn_frame, text="Fechar", command=master.quit, bootstyle="danger").pack(side="left", padx=10)

        ttk.Label(self.frame_geral, text="Ações Rápidas", font=('Segoe UI', 12, 'bold')).pack(pady=20)
        for action in ["Buscar Promoções", "Enviar Vendas", "Enviar Fechamentos"]:
            btn = ttk.Button(self.frame_geral, text=action, bootstyle="primary", width=20)
            btn.pack(pady=10)
            ToolTip(btn, text=f"Executa a ação: {action.lower()}", bootstyle="inverse")

        self.campos["Habilitar Integração Scanntech"].trace_add("write", self.atualizar_botoes_geral)
        self.carregar_configuracoes()
        self.atualizar_botoes_geral()
        canvas.bind_all("<MouseWheel>", lambda event: canvas.yview_scroll(int(-1 * (event.delta / 120)), "units"))

    def formatar_data(self, event):
        if event.keysym in ("BackSpace", "Delete", "Left", "Right", "Up", "Down", "Tab"):
            return
        if event.char and not event.char.isdigit():
            return "break"

        entry = event.widget
        text = entry.get()
        numeros = "".join(filter(str.isdigit, text + event.char))
        
        if len(numeros) > 8:
            return "break"

        formatted = ""
        if len(numeros) > 4:
            formatted = f"{numeros[:2]}/{numeros[2:4]}/{numeros[4:]}"
        elif len(numeros) > 2:
            formatted = f"{numeros[:2]}/{numeros[2:]}"
        else:
            formatted = numeros

        entry.delete(0, tk.END)
        entry.insert(0, formatted)
        return "break"

    def validar_data(self, data_str):
        try:
            if not data_str:
                return False, "A data não pode estar vazia."
            datetime.strptime(data_str, '%d/%m/%Y')
            return True, ""
        except ValueError:
            return False, f"Formato inválido. Use DD/MM/YYYY."

    def carregar_configuracoes(self):
        try:
            config = carregar_configuracoes()
            if not config:
                logging.warning("Nenhuma configuração encontrada ou arquivo vazio. Usando valores padrão.")
                return

            mapeamento = {
                "integração scanntech": "Habilitar Integração Scanntech",
                "usuário": "Usuário", "senha": "Senha",
                "url 1": "URL 1", "url 2": "URL 2", "url 3": "URL 3",
                "empresa": "Empresa", "local": "Local", "crm": "CRM",
                "código da empresa": "Código da empresa",
                "intervalo (s)": "Intervalo (s)",
                "data de início": "Data de Início",
                "data início envio de vendas": "Data início envio de vendas",
                "data início envio de fechamentos": "Data início envio de fechamentos",
            }

            for chave_config, chave_campo in mapeamento.items():
                if chave_config in config:
                    valor = config[chave_config]
                    if chave_campo == "Senha":
                        self.senha_real = valor 
                        self.campos[chave_campo].set("********")
                    elif chave_campo == "Habilitar Integração Scanntech":
                        self.campos[chave_campo].set(valor.lower() == "true")
                    else:
                        self.campos[chave_campo].set(valor)
        except FileNotFoundError:
             logging.info(f"Arquivo settings.config não encontrado. Será criado ao salvar.")
        except Exception as e:
            logging.error(f"Erro ao carregar configurações: {str(e)}")
            messagebox.showerror("Erro", f"Falha ao carregar configurações:\n{str(e)}")

    def salvar_config(self):
        essenciais = ["Usuário", "Senha", "URL 1", "Empresa", "Local", "CRM", "Código da empresa"]
        for campo in essenciais:
            # Não verifica a senha se ela já estiver preenchida como '********'
            if campo == "Senha" and self.campos[campo].get() == "********":
                continue
            if not self.campos[campo].get().strip():
                messagebox.showerror("Erro", f"O campo '{campo}' é obrigatório.")
                return

        for campo_data in ["Data de Início", "Data início envio de vendas", "Data início envio de fechamentos"]:
            data = self.campos[campo_data].get()
            valido, mensagem = self.validar_data(data)
            if not valido:
                messagebox.showerror("Erro", f"Campo '{campo_data}' inválido: {mensagem}")
                return

        senha_input = self.entry_senha.get()
        senha_para_salvar = self.senha_real if senha_input == "********" and hasattr(self, 'senha_real') else senha_input

        config_texto = "[SCANNTECH]\n"
        mapeamento = {
            "Habilitar Integração Scanntech": "integração scanntech",
            "Usuário": "usuário", "Senha": "senha",
            "URL 1": "url 1", "URL 2": "url 2", "URL 3": "url 3",
            "Empresa": "empresa", "Local": "local", "CRM": "crm",
            "Código da empresa": "código da empresa",
            "Intervalo (s)": "intervalo (s)",
            "Data de Início": "data de início",
            "Data início envio de vendas": "data início envio de vendas",
            "Data início envio de fechamentos": "data início envio de fechamentos",
        }
        for k, v in self.campos.items():
            valor = str(v.get()).lower() if k == "Habilitar Integração Scanntech" else (senha_para_salvar if k == "Senha" else v.get())
            config_texto += f"{mapeamento[k]} = {valor}\n"

        try:
            # Garante que o diretório exista antes de tentar escrever o arquivo
            os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
            with open(CONFIG_PATH, "w", encoding="utf-8") as configfile:
                configfile.write(config_texto)
            
            messagebox.showinfo("Sucesso", "Configuração salva com sucesso!")
            
            # Tenta criar/atualizar estruturas do banco após salvar
            try:
                criar_tabelas_scanntech(self.master)
                config_dict = carregar_configuracoes()
                gerar_fechamentos_pendentes(config_dict)
            except Exception as e:
                messagebox.showwarning("Aviso", f"Configuração salva, mas falha ao criar estrutura no banco:\n{str(e)}")

        except Exception as e:
            logging.error(f"Erro ao salvar configurações: {str(e)}")
            messagebox.showerror("Erro", f"Falha ao salvar configurações:\n{str(e)}")

    def somente_numeros(self, valor):
        return valor.isdigit() or valor == ""

    def centralizar_janela(self, janela, largura=600, altura=600):
        janela.update_idletasks()
        largura_tela = janela.winfo_screenwidth()
        altura_tela = janela.winfo_screenheight()
        x = (largura_tela // 2) - (largura // 2)
        y = (altura_tela // 2) - (altura // 2)
        janela.geometry(f"{largura}x{altura}+{x}+{y}")

if __name__ == "__main__":
    root = ttk.Window(themename="lumen")
    root.withdraw()

    if validar_config_hos():
        root.deiconify()
        app = ConfiguradorApp(root)
        root.resizable(False, False)
        root.mainloop()
    else:
        root.destroy()