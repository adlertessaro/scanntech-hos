from scanntech.db.conexao import conectar
from tkinter import messagebox
from pathlib import Path
import sys
import os
import logging # Adicionado para logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Função para acessar recursos empacotados (ex: create_tables.sql)
def resource_path(relative_path):
    """ 
    Retorna o caminho absoluto para um recurso, de forma que funcione tanto em
    modo de desenvolvimento quanto em um executável do PyInstaller.
    """
    try:
        # PyInstaller cria uma pasta temporária e armazena o caminho em sys._MEIPASS
        # Os arquivos de dados (como .sql) devem ser incluídos no arquivo .spec
        base_path = Path(sys._MEIPASS)
    except AttributeError:
        # sys._MEIPASS não existe, então não estamos em um executável (modo de desenvolvimento).
        # Assumimos que a estrutura do projeto é:
        # /<project_root>/
        #   - /config/ (onde este script, setup_db.py, está)
        #   - /db/ (onde o recurso relativo está)
        # Path(__file__).resolve().parent nos dá a pasta /config
        # .parent nos leva para /<project_root>
        base_path = Path(__file__).resolve().parent.parent

    # Retorna o caminho completo para o recurso (ex: /<project_root>/db/create_tables.sql)
    return base_path / Path(relative_path)

def executar_script(path, cur):
    """
    Executa um script SQL inteiro de um arquivo.
    Este método não divide mais o script por ';', o que evita erros em
    funções e triggers que usam ';' internamente.
    """
    with open(path, "r", encoding="utf-8") as file:
        sql_script = file.read()
        if sql_script.strip(): # Verifica se o script não está vazio
            try:
                cur.execute(sql_script)
                logging.info(f"Script {path.name} executado com sucesso.")
            except Exception as e:
                erro = str(e).lower()
                
                # Condições para ignorar o erro de forma segura, com suporte a PT-BR
                is_already_exists_error = (
                    "already exists" in erro 
                    or "duplicate column" in erro 
                    or "já existe" in erro # Suporte para erros em português
                )

                if is_already_exists_error:
                    logging.warning(f"Ignorando erros de 'já existe' no script {path.name}.")
                else:
                    # Se for um erro diferente, lança a exceção
                    logging.error(f"Erro inesperado ao executar o script {path.name}")
                    raise e


def criar_tabelas_scanntech(root=None):
    """
    Conecta ao banco de dados e executa os scripts para criar/atualizar a estrutura de tabelas e triggers.
    Usa o modo autocommit para garantir que cada declaração seja executada independentemente.
    """
    conn = None # Inicializa a variável de conexão
    try:
        conn = conectar()
        # Ativa o modo autocommit. Isso impede que um erro em uma declaração
        # invalide toda a transação, tratando cada comando como uma transação separada.
        conn.autocommit = True
        cur = conn.cursor()

        # Obtém o caminho para os scripts SQL de forma dinâmica
        script_tabelas = resource_path("db/create_tables.sql")
        script_triggers = resource_path("db/create_triggers.sql")

        logging.info(f"Iniciando a execução do script de tabelas em: {script_tabelas}")
        executar_script(script_tabelas, cur)
        
        logging.info(f"Iniciando a execução do script de triggers em: {script_triggers}")
        executar_script(script_triggers, cur)

        # Em modo autocommit, não é necessário chamar conn.commit()
        logging.info("Estrutura do banco de dados criada/verificada com sucesso.")

        if root:
            messagebox.showinfo("Sucesso", "Estrutura do banco de dados verificada/criada com sucesso!")
        
    except FileNotFoundError as e:
        logging.error(f"Arquivo de script SQL não encontrado: {e}")
        if root:
            messagebox.showerror("Erro de Arquivo", f"Não foi possível encontrar um arquivo de script necessário:\n{e}")
        else:
            raise e
            
    except Exception as e:
        logging.error(f"Falha crítica ao configurar o banco de dados: {e}")
        if root:
            messagebox.showerror("Erro ao Configurar Banco de Dados", f"Ocorreu um erro crítico:\n{e}")
        else:
            # Se não houver interface, lança a exceção para que o chamador possa tratar
            raise e
    finally:
        # Garante que a conexão seja fechada
        if conn:
            cur.close()
            conn.close()

