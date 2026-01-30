import ctypes
import tkinter as tk
from tkinter import messagebox
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from ttkbootstrap.tooltip import ToolTip
from datetime import datetime
import os
import sys
from configparser import ConfigParser
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))
LOG_DIR = ROOT_DIR / "logs" 
from scanntech.db.promo_repo import salvar_e_processar_promocoes
from scanntech.config.settings import carregar_configuracoes, CONFIG_PATH
from scanntech.models.gerar_fechamentos_pendentes import gerar_fechamentos_pendentes
# from scanntech.services.promocoes_service import processar_promocoes
from scanntech.db.vendas_repo import forcar_envio_vendas_com_verificacao
from scanntech.db.fechamentos_repo import forcar_envio_fechamentos_com_verificacao
from scanntech.config.setup_db import criar_tabelas_scanntech
sys.path.append(str(Path(__file__).parent.parent.parent))
import json
import logging
import threading

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

def validar_config_hos():
    # (sem alterações)
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

# --- NOVA CLASSE PARA JANELA DE ESPERA ---
class JanelaAguarde(ttk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Processando")
        self.transient(parent)
        self.grab_set()
        
        # Centraliza a janela de espera no pai
        self.update_idletasks()
        parent_x = parent.winfo_x()
        parent_y = parent.winfo_y()
        parent_width = parent.winfo_width()
        parent_height = parent.winfo_height()
        width = 250
        height = 100
        x = parent_x + (parent_width // 2) - (width // 2)
        y = parent_y + (parent_height // 2) - (height // 2)
        self.geometry(f"{width}x{height}+{x}+{y}")
        
        self.protocol("WM_DELETE_WINDOW", lambda: None) # Impede de fechar pelo "X"
        self.resizable(False, False)

        label = ttk.Label(self, text="Processando... por favor, aguarde.", bootstyle="info")
        label.pack(pady=10, padx=20)
        
        progress = ttk.Progressbar(self, mode='indeterminate', bootstyle="striped-info")
        progress.pack(pady=10, padx=20, fill='x')
        progress.start()

class JanelaLoja(ttk.Toplevel):
    # (sem alterações nesta classe)
    def __init__(self, parent, treeview, item_selecionado=None):
        super().__init__(parent)
        self.treeview = treeview
        self.item_selecionado = item_selecionado
        
        action_text = "Editar Loja" if item_selecionado else "Adicionar Nova Loja"
        self.title(action_text)
        self.transient(parent); self.grab_set()

        main_frame = ttk.Frame(self, padding=15)
        main_frame.pack(fill=BOTH, expand=True)
        main_frame.columnconfigure(1, weight=1)

        ttk.Label(main_frame, text="ID Empresa (Scanntech):").grid(row=0, column=0, sticky="w", pady=5)
        self.entry_id_empresa_scann = ttk.Entry(main_frame)
        self.entry_id_empresa_scann.grid(row=0, column=1, sticky="ew", padx=(5, 0))
        ToolTip(self.entry_id_empresa_scann, text="ID da Empresa fornecido pela Scanntech (Ex: 77).", bootstyle="inverse")

        ttk.Label(main_frame, text="ID Local (Scanntech):").grid(row=1, column=0, sticky="w", pady=5)
        self.entry_id_local_scann = ttk.Entry(main_frame)
        self.entry_id_local_scann.grid(row=1, column=1, sticky="ew", padx=(5, 0))
        ToolTip(self.entry_id_local_scann, text="ID da Loja/Filial fornecido pela Scanntech (Ex: 1).", bootstyle="inverse")

        ttk.Label(main_frame, text="Código Empresa (ERP):").grid(row=2, column=0, sticky="w", pady=5)
        self.entry_empresa_erp = ttk.Entry(main_frame)
        self.entry_empresa_erp.grid(row=2, column=1, sticky="ew", padx=(5, 0))
        ToolTip(self.entry_empresa_erp, text="Código que identifica a empresa no seu sistema ERP.", bootstyle="inverse")
    

        if item_selecionado:
            valores = self.treeview.item(item_selecionado, 'values')
            self.entry_id_empresa_scann.insert(0, valores[0])
            self.entry_id_local_scann.insert(0, valores[1])
            self.entry_empresa_erp.insert(0, valores[2])

        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=(10, 15))
        ttk.Button(btn_frame, text="Salvar", command=self.salvar, bootstyle="primary").pack(side=LEFT, padx=10)
        ttk.Button(btn_frame, text="Cancelar", command=self.destroy, bootstyle="primary").pack(side=LEFT, padx=10)

        self._centralizar(parent)
        
    def _centralizar(self, parent):
        self.update_idletasks()
        parent_x = parent.winfo_x()
        parent_y = parent.winfo_y()
        parent_width = parent.winfo_width()
        parent_height = parent.winfo_height()
        
        width = self.winfo_width()
        height = self.winfo_height()
        
        x = parent_x + (parent_width // 2) - (width // 2)
        y = parent_y + (parent_height // 2) - (height // 2)
        
        self.geometry(f"450x250+{x}+{y}")

    def salvar(self):
        id_empresa_scann = self.entry_id_empresa_scann.get().strip()
        id_local_scann = self.entry_id_local_scann.get().strip()
        empresa_erp = self.entry_empresa_erp.get().strip()

        if not all([id_empresa_scann, id_local_scann, empresa_erp]):
            messagebox.showerror("Erro de Validação", "Todos os campos são obrigatórios.", parent=self)
            return
        
        novos_valores = (id_empresa_scann, id_local_scann, empresa_erp)

        if self.item_selecionado:
            self.treeview.item(self.item_selecionado, values=novos_valores)
        else:
            self.treeview.insert("", "end", values=novos_valores)
            
        self.destroy()

class ConfiguradorApp:
    # --- __init__ e outras funções de criação de widgets sem alterações ---
    def __init__(self, master):
        self.master = master
        master.title("Configurador Scanntech")
        try:
            # Reutiliza a mesma lógica do monitor para encontrar o ícone
            caminho_icone = ROOT_DIR / "scanntech" / "core" / "logo.ico"
            if caminho_icone.exists():
                master.iconbitmap(caminho_icone)
            else:
                # O logging já está configurado neste arquivo, então vamos usá-lo
                logging.warning(f"Arquivo de ícone não encontrado em: {caminho_icone}")
        except Exception as e:
            logging.warning(f"Não foi possível carregar o ícone. Erro: {e}")

        if sys.platform == 'win32':
            try:
                # Define um ID único para a aplicação
                app_id = 'scanntech.integrador.configurador.1'
                ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(app_id)
            except Exception as e:
                logging.warning(f"Não foi possível definir o ícone da barra de tarefas. Erro: {e}")
            
        self.centralizar_janela(master, 800, 700)
        self.style = ttk.Style(theme="lumen")

        self.notebook = ttk.Notebook(master)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)

        self.tab_geral = ttk.Frame(self.notebook, padding=10)
        self.tab_lojas = ttk.Frame(self.notebook, padding=10)
        self.tab_acoes = ttk.Frame(self.notebook, padding=10)

        self.notebook.add(self.tab_geral, text="Configurações Gerais")
        self.notebook.add(self.tab_lojas, text="Lojas")
        self.notebook.add(self.tab_acoes, text="Ações Manuais")
        
        self.campos_gerais = {
            "Habilitar Integração Scanntech": tk.BooleanVar(value=True),
            "Usuário": tk.StringVar(), "Senha": tk.StringVar(),
            "URL 1": tk.StringVar(), "URL 2": tk.StringVar(), "URL 3": tk.StringVar(),
            "Intervalo (s)": tk.StringVar(value="1800"),
            "Data de Início": tk.StringVar(value=datetime.now().strftime("%d/%m/%Y")),
            "Data início envio de fechamentos": tk.StringVar(value=datetime.now().strftime("%d/%m/%Y"))
        }

        self.criar_widgets_gerais()
        self.criar_widgets_lojas()
        self.criar_widgets_acoes()

        btn_frame = ttk.Frame(master)
        btn_frame.pack(fill='x', padx=10, pady=(0, 10))
        ttk.Button(btn_frame, text="Salvar Configurações", command=self.salvar_config, bootstyle="primary").pack(side="left", padx=10)
        ttk.Button(btn_frame, text="Fechar", command=master.quit, bootstyle="primary").pack(side="right", padx=10)
        
        self.carregar_configuracoes()
    
    def criar_widgets_gerais(self):
        integracao_frame = ttk.Labelframe(self.tab_geral, text="Integração", padding=10)
        integracao_frame.pack(fill='x', padx=5, pady=5)
        check_integracao = ttk.Checkbutton(integracao_frame, text="Habilitar Integração Scanntech", variable=self.campos_gerais["Habilitar Integração Scanntech"], bootstyle="round-toggle")
        check_integracao.pack(anchor="w", pady=5)
        ToolTip(check_integracao, text="Ativa ou desativa a integração com a Scanntech.", bootstyle="inverse")

        credenciais_frame = ttk.Labelframe(self.tab_geral, text="Credenciais e Conexão", padding=10)
        credenciais_frame.pack(fill='x', padx=5, pady=5)
        credenciais_frame.columnconfigure(1, weight=1)
        
        campos = {
            "Usuário": "Usuário de acesso à API.", "Senha": "Senha de acesso à API.",
            "URL 1": "URL principal da API Scanntech.", "URL 2": "URL secundária (backup 1).", "URL 3": "URL terciária (backup 2).",
            "Intervalo (s)": "Intervalo em segundos entre as execuções do serviço de envio."
        }
        
        for i, (label, tooltip_text) in enumerate(campos.items()):
            ttk.Label(credenciais_frame, text=f"{label}:").grid(row=i, column=0, sticky="w", padx=5, pady=5)
            entry = ttk.Entry(credenciais_frame, textvariable=self.campos_gerais[label])
            if label == "Senha":
                entry.config(show="*")
                self.entry_senha = entry
            entry.grid(row=i, column=1, sticky="ew", padx=5)
            ToolTip(entry, text=tooltip_text, bootstyle="inverse")

        datas_frame = ttk.Labelframe(self.tab_geral, text="Datas de Início do Processamento", padding=10)
        datas_frame.pack(fill='x', padx=5, pady=5, expand=True)
        datas_frame.columnconfigure(1, weight=1)

        ttk.Label(datas_frame, text="Vendas (Geral):").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        entry_data_inicio = ttk.Entry(datas_frame, textvariable=self.campos_gerais["Data de Início"])
        entry_data_inicio.grid(row=0, column=1, sticky="ew", padx=5)
        ToolTip(entry_data_inicio, text="Data inicial para busca de vendas pendentes (DD/MM/AAAA)", bootstyle="inverse")

        ttk.Label(datas_frame, text="Fechamentos:").grid(row=1, column=0, sticky="w", pady=5)
        entry_data_fech = ttk.Entry(datas_frame, textvariable=self.campos_gerais["Data início envio de fechamentos"])
        entry_data_fech.grid(row=1, column=1, sticky="ew", padx=5)
        ToolTip(entry_data_fech, text="Data inicial para envio de fechamentos (DD/MM/AAAA)", bootstyle="inverse")

    def criar_widgets_lojas(self):
        frame_lojas = ttk.Frame(self.tab_lojas)
        frame_lojas.pack(fill=BOTH, expand=True)

        cols = ("ID Empresa Scanntech", "ID Local Scanntech", "Código Empresa ERP")
        self.tree = ttk.Treeview(frame_lojas, columns=cols, show='headings', bootstyle="primary")
        for col in cols:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=150, anchor=CENTER)
        
        self.tree.pack(side=LEFT, fill=BOTH, expand=True)

        scrollbar = ttk.Scrollbar(frame_lojas, orient=VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=RIGHT, fill=Y)

        botoes_loja_frame = ttk.Frame(self.tab_lojas)
        botoes_loja_frame.pack(fill=X, pady=10)
        ttk.Button(botoes_loja_frame, text="Adicionar", command=self.adicionar_loja, bootstyle="primary").pack(side=LEFT, padx=5)
        ttk.Button(botoes_loja_frame, text="Editar", command=self.editar_loja, bootstyle="primary").pack(side=LEFT, padx=5)
        ttk.Button(botoes_loja_frame, text="Remover", command=self.remover_loja, bootstyle="primary").pack(side=LEFT, padx=5)

    def criar_widgets_acoes(self):
        ttk.Label(self.tab_acoes, text="Execute ações manuais para uma ou todas as lojas:", justify="center").pack(pady=(10, 5))
        
        frame_selecao = ttk.Frame(self.tab_acoes)
        frame_selecao.pack(fill=X, expand=True, pady=5)
        
        cols_acoes = ("ID Empresa Scanntech", "ID Local Scanntech", "Código Empresa ERP")
        self.tree_acoes = ttk.Treeview(frame_selecao, columns=cols_acoes, show='headings', height=5)
        for col in cols_acoes:
            self.tree_acoes.heading(col, text=col)
            self.tree_acoes.column(col, width=120, anchor=CENTER)
        self.tree_acoes.pack(side=LEFT, fill=X, expand=True)
        ToolTip(self.tree_acoes, text="Selecione uma loja.", bootstyle="inverse")

        scrollbar_acoes = ttk.Scrollbar(frame_selecao, orient=VERTICAL, command=self.tree_acoes.yview)
        self.tree_acoes.configure(yscrollcommand=scrollbar_acoes.set)
        scrollbar_acoes.pack(side=RIGHT, fill=Y)

        botoes_acoes_frame = ttk.Frame(self.tab_acoes)
        botoes_acoes_frame.pack(pady=10)
        
        self.botoes_acoes = {}
        # Parte do codigo comentada para que não seja utilizado as promoções até as promoçoes multiplas do HOS Farma esta pronta.
        # self.botoes_acoes["promocoes"] = ttk.Button(botoes_acoes_frame, text="Buscar Promoções", command=lambda: self.executar_acao('promocoes'), bootstyle="primary")
        # self.botoes_acoes["promocoes"].pack(side=LEFT, padx=10)
        # ToolTip(self.botoes_acoes["promocoes"], text="Busca promoções de todas as lojas cadastradas.", bootstyle="inverse")
        self.botoes_acoes["vendas"] = ttk.Button(botoes_acoes_frame, text="Forçar Envio de Vendas", command=lambda: self.executar_acao('vendas'), bootstyle="primary")
        self.botoes_acoes["vendas"].pack(side=LEFT, padx=10)
        ToolTip(self.botoes_acoes["vendas"], text="Força envio de vendas pendentes da loja selecionada.", bootstyle="inverse")
        self.botoes_acoes["fechamentos"] = ttk.Button(botoes_acoes_frame, text="Forçar Envio de Fechamentos", command=lambda: self.executar_acao('fechamentos'), bootstyle="primary")
        self.botoes_acoes["fechamentos"].pack(side=LEFT, padx=10)
        ToolTip(self.botoes_acoes["fechamentos"], text="Força envio de fechamentos pendentes da loja selecionada.", bootstyle="inverse")
    
    # --- LÓGICA DE AÇÕES MANUAIS TOTALMENTE REESTRUTURADA ---
    def executar_acao(self, acao):
        """Orquestra a execução da ação, mostrando a janela de espera e usando uma thread."""
        
        # Define a função de trabalho e os argumentos com base na ação
        if acao == 'promocoes':
            if not self.tree_acoes.get_children():
                messagebox.showwarning("Nenhuma Loja", "Não há lojas cadastradas para buscar promoções.")
                return
            msg = f"Deseja buscar promoções para TODAS as {len(self.tree_acoes.get_children())} lojas cadastradas?"
            if not messagebox.askyesno("Confirmar Ação em Massa", msg):
                return
            tarefa = self._tarefa_buscar_promocoes_todas_lojas
            args = ()
        else: # Ações de 'vendas' e 'fechamentos'
            selecionado = self.tree_acoes.selection()
            if not selecionado:
                messagebox.showwarning("Nenhuma Seleção", f"Selecione uma loja na lista para executar a ação de '{acao}'.")
                return
            
            valores = self.tree_acoes.item(selecionado[0], 'values')
            loja_str = f"Loja {valores[1]} da Empresa {valores[0]} (ERP: {valores[2]})"
            
            if not messagebox.askyesno("Confirmar Ação", f"Deseja executar a ação '{acao}' para:\n{loja_str}?"):
                return
            tarefa = self._tarefa_acao_loja_unica
            args = (acao, valores)

        # Inicia a interface de espera e a thread
        janela_aguarde = JanelaAguarde(self.master)
        resultado_final = [] # Usamos uma lista para que a thread possa modificar seu conteúdo
        
        thread = threading.Thread(target=tarefa, args=args + (resultado_final,))
        thread.start()
        
        # Inicia a verificação do status da thread
        self.master.after(100, self._verificar_thread, thread, janela_aguarde, resultado_final)

    def _verificar_thread(self, thread, janela_aguarde, resultado_final):
        """Verifica se a thread terminou. Se sim, mostra o resultado. Se não, agenda nova verificação."""
        if thread.is_alive():
            self.master.after(100, self._verificar_thread, thread, janela_aguarde, resultado_final)
        else:
            janela_aguarde.destroy()
            if resultado_final:
                titulo, mensagem = resultado_final[0]
                if "Falha" in titulo or "Erro" in titulo:
                    messagebox.showerror(titulo, mensagem)
                else:
                    messagebox.showinfo(titulo, mensagem)
    
    def _tarefa_buscar_promocoes_todas_lojas(self, resultado_final):
        """Função executada na thread para buscar promoções de todas as lojas e depois salvar."""
        try:
            configs = carregar_configuracoes()
            config_geral = configs.get('geral', {})
            todas_as_lojas_config = configs.get('lojas', [])
            
            sucessos = []
            falhas = []
            promocoes_agrupadas_para_salvar = {}

            # Etapa 1: Coletar promoções de todas as lojas
            for loja_config in todas_as_lojas_config:
                id_loja = loja_config.get('idlocal', 'Desconhecida')
                try:
                    # --- ALTERAÇÃO PRINCIPAL ---
                    # Combinamos a config geral com a da loja específica em um único dicionário.
                    # Isso garante que a função `processar_promocoes` receba todas as informações
                    # necessárias, padronizando com o fluxo de loja única que já funciona.
                    config_completa_loja = {**config_geral, **loja_config}
                    
                    # Passamos o dicionário combinado para o serviço.
                    resultado_loja = processar_promocoes(config_completa_loja)
                    
                    if resultado_loja:
                        promocoes_agrupadas_para_salvar.update(resultado_loja)
                    sucessos.append(id_loja)
                except Exception as e:
                    logging.error(f"Falha na API ao buscar promoções para loja {id_loja}: {e}")
                    falhas.append(f"Loja {id_loja} (API): {e}")

            # Etapa 2: Salvar todas as promoções coletadas de uma vez (sem alterações aqui)
            if promocoes_agrupadas_para_salvar:
                try:
                    salvar_e_processar_promocoes(promocoes_agrupadas_para_salvar)
                    logging.info("Promoções salvas no banco de dados com sucesso.")
                except Exception as e:
                    logging.error(f"Falha ao salvar promoções no banco de dados: {e}")
                    falhas.append(f"Banco de Dados: {e}")
            
            # Etapa 3: Montar o relatório final (sem alterações aqui)
            relatorio = f"Busca e salvamento de promoções concluído!\n\nLojas consultadas com sucesso: {len(sucessos)}\nLojas com falha: {len(falhas)}."
            if falhas:
                relatorio += "\n\nDetalhes das falhas:\n" + "\n".join(falhas)
            
            resultado_final.append(("Resultado da Busca", relatorio))
        except Exception as e:
            logging.critical(f"Erro crítico na thread de busca de promoções: {e}")
            resultado_final.append(("Erro Crítico", f"Ocorreu uma falha inesperada:\n{e}"))


    def _tarefa_acao_loja_unica(self, acao, valores_loja, resultado_final):
        # (sem alterações)
        id_empresa, id_local, erp_code = valores_loja
        configs = carregar_configuracoes()
        config_geral = configs.get('geral', {})
        lojas = configs.get('lojas', [])
        config_loja_especifica = next((l for l in lojas if l.get('idempresa') == id_empresa and l.get('idlocal') == id_local and l.get('empresa') == erp_code), None)
        
        if not config_loja_especifica:
            resultado_final.append(("Erro", "Configuração da loja selecionada não foi encontrada."))
            return

        config_completa = {**config_geral, **config_loja_especifica}
        try:
            if acao == 'vendas':
                resultado = forcar_envio_vendas_com_verificacao(config_completa)
            elif acao == 'fechamentos':
                resultado = forcar_envio_fechamentos_com_verificacao(config_completa)
            resultado_final.append(("Sucesso", resultado))
        except Exception as e:
            resultado_final.append(("Falha na Execução", f"Ocorreu um erro ao executar a ação '{acao}':\n{e}"))

    def adicionar_loja(self):
        JanelaLoja(self.master, self.tree)

    def editar_loja(self):
        selecionado = self.tree.selection()
        if not selecionado:
            messagebox.showwarning("Nenhuma Seleção", "Por favor, selecione uma loja para editar.")
            return
        JanelaLoja(self.master, self.tree, selecionado[0])

    def remover_loja(self):
        selecionado = self.tree.selection()
        if not selecionado:
            messagebox.showwarning("Nenhuma Seleção", "Por favor, selecione uma loja para remover.")
            return
        if messagebox.askyesno("Confirmar Remoção", "Tem certeza que deseja remover a loja selecionada?"):
            self.tree.delete(selecionado[0])
    
    def carregar_configuracoes(self):
        try:
            config = carregar_configuracoes()
            config_geral = config.get('geral', {})
            lojas = config.get('lojas', [])

            if not config_geral:
                logging.warning("Nenhuma configuração geral encontrada.")
                return

            self.campos_gerais["Habilitar Integração Scanntech"].set(config_geral.get("habilitar_integracao_scanntech", "true").lower() == "true")
            self.campos_gerais["Usuário"].set(config_geral.get("usuario", ""))
            self.senha_real = config_geral.get("senha", "")
            self.campos_gerais["Senha"].set("********" if self.senha_real else "")
            self.campos_gerais["URL 1"].set(config_geral.get("url_1", ""))
            self.campos_gerais["URL 2"].set(config_geral.get("url_2", ""))
            self.campos_gerais["URL 3"].set(config_geral.get("url_3", ""))
            self.campos_gerais["Intervalo (s)"].set(config_geral.get("intervalo_s", "1800"))
            self.campos_gerais["Data de Início"].set(config_geral.get("data_de_inicio", datetime.now().strftime("%d/%m/%Y")))
            self.campos_gerais["Data início envio de fechamentos"].set(config_geral.get("data_inicio_envio_de_fechamentos", datetime.now().strftime("%d/%m/%Y")))
            
            for tree in [self.tree, self.tree_acoes]:
                for i in tree.get_children():
                    tree.delete(i)
            
            for loja in lojas:
                valores = (
                    loja.get("idempresa", ""),
                    loja.get("idlocal", ""),
                    loja.get("empresa", "")
                )
                self.tree.insert("", "end", values=valores)
                
                valores_acoes = (valores[0], valores[1], valores[2])
                self.tree_acoes.insert("", "end", values=valores_acoes)

        except Exception as e:
            logging.error(f"Erro ao carregar configurações: {e}")
            messagebox.showerror("Erro de Carregamento", f"Falha ao carregar o arquivo de configurações:\n{e}")

    def salvar_config(self):
        parser = ConfigParser()
        
        parser.add_section("SCANNTECH_GERAL")
        senha_input = self.entry_senha.get()
        senha_para_salvar = self.senha_real if senha_input == "********" and hasattr(self, 'senha_real') else senha_input
        
        parser.set("SCANNTECH_GERAL", "habilitar_integracao_scanntech", str(self.campos_gerais["Habilitar Integração Scanntech"].get()))
        parser.set("SCANNTECH_GERAL", "usuario", self.campos_gerais["Usuário"].get())
        parser.set("SCANNTECH_GERAL", "senha", senha_para_salvar)
        parser.set("SCANNTECH_GERAL", "url_1", self.campos_gerais["URL 1"].get())
        parser.set("SCANNTECH_GERAL", "url_2", self.campos_gerais["URL 2"].get())
        parser.set("SCANNTECH_GERAL", "url_3", self.campos_gerais["URL 3"].get())
        parser.set("SCANNTECH_GERAL", "intervalo_s", self.campos_gerais["Intervalo (s)"].get())
        parser.set("SCANNTECH_GERAL", "data_de_inicio", self.campos_gerais["Data de Início"].get())
        parser.set("SCANNTECH_GERAL", "data_inicio_envio_de_fechamentos", self.campos_gerais["Data início envio de fechamentos"].get())

        for i, item_id in enumerate(self.tree.get_children()):
            valores = self.tree.item(item_id, 'values')
            section_name = f"LOJA_{i+1}"
            parser.add_section(section_name)
            parser.set(section_name, "idempresa", valores[0])
            parser.set(section_name, "idlocal", valores[1])
            parser.set(section_name, "empresa", valores[2])

        try:
            # --- ALTERAÇÃO PRINCIPAL ---
            # Garante que a pasta de logs exista antes de salvar o arquivo de configuração
            LOG_DIR.mkdir(exist_ok=True)
            logging.info(f"Pasta de logs verificada/criada em: {LOG_DIR}")
            
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                parser.write(f)
            messagebox.showinfo("Sucesso", "Configurações salvas com sucesso!")
            
            try:
                criar_tabelas_scanntech(self.master)
            except Exception as e:
                messagebox.showwarning("Aviso", f"Configuração salva, mas falha ao criar/verificar estrutura no banco:\n{str(e)}")

        except Exception as e:
            logging.error(f"Erro ao salvar configurações: {e}")
            messagebox.showerror("Erro", f"Falha ao salvar as configurações:\n{e}")

    def centralizar_janela(self, janela, largura, altura):
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