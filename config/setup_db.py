from scanntech.db.conexao import conectar
from tkinter import messagebox
from pathlib import Path
import sys
import os
import logging
import psycopg2.errors # Importa os erros específicos do psycopg2

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def resource_path(relative_path):
    """ Retorna o caminho absoluto para um recurso. """
    try:
        base_path = Path(sys._MEIPASS)
    except AttributeError:
        base_path = Path(__file__).resolve().parent.parent
    return base_path / Path(relative_path)

def executar_script(path, cur, conn, is_trigger_script=False):
    """
    Executa um script SQL, tratando erros de "objeto já existe" de forma robusta
    e gerenciando a transação.
    """
    with open(path, "r", encoding="utf-8") as file:
        full_script = file.read()

    commands = [full_script] if is_trigger_script else full_script.split(';')

    for command in commands:
        if command.strip():
            try:
                cur.execute(command)
            except (psycopg2.errors.DuplicateTable, psycopg2.errors.DuplicateObject, psycopg2.errors.DuplicateFunction) as e:
                # Captura erros específicos de objetos duplicados
                logging.warning(f"Objeto já existe, ignorando comando: {command.strip()[:100]}...")
                conn.rollback() # Desfaz a transação falha para poder continuar
            except Exception as e:
                logging.error(f"Erro crítico ao executar o comando: {command.strip()[:100]}...")
                conn.rollback() # Desfaz a transação em caso de erro crítico
                raise e

    logging.info(f"Script {path.name} executado com sucesso.")

def aplicar_migracoes_promocoes(cur, conn):
    """
    Garante que a tabela 'promocao_cab' tenha a coluna 'id_scanntech' e a restrição UNIQUE.
    """
    logging.info("Verificando e aplicando migrações para a tabela 'promocao_cab'...")
    try:
        cur.execute("ALTER TABLE promocao_cab ADD COLUMN id_scanntech VARCHAR(255);")
        logging.info("Coluna 'id_scanntech' adicionada com sucesso em 'promocao_cab'.")
    except psycopg2.errors.DuplicateColumn:
        logging.info("Coluna 'id_scanntech' já existe em 'promocao_cab'. Nenhuma ação necessária.")
        conn.rollback() # Importante para continuar a execução
    
    try:
        cur.execute("ALTER TABLE promocao_cab ADD CONSTRAINT uk_promocao_cab_id_scanntech UNIQUE (id_scanntech);")
        logging.info("Restrição UNIQUE para 'id_scanntech' adicionada com sucesso.")
    except psycopg2.errors.DuplicateTable: # Erro para constraint já existente é DuplicateTable
        logging.info("Restrição UNIQUE para 'id_scanntech' já existe. Nenhuma ação necessária.")
        conn.rollback() # Importante para continuar a execução

def criar_tabelas_scanntech(root=None):
    """
    Conecta ao banco, cria as tabelas, aplica migrações e depois cria as triggers.
    Gerencia a transação manualmente para maior controle.
    """
    conn = None
    try:
        conn = conectar()
        cur = conn.cursor()
        
        script_tabelas = resource_path("db/create_tables.sql")
        script_triggers = resource_path("db/create_triggers.sql")

        logging.info(f"Executando script de criação de tabelas de integração: {script_tabelas}")
        executar_script(script_tabelas, cur, conn)

        aplicar_migracoes_promocoes(cur, conn)

        logging.info(f"Executando script de criação de triggers: {script_triggers}")
        executar_script(script_triggers, cur, conn, is_trigger_script=True)
        
        conn.commit() # Efetiva todas as alterações se tudo correu bem
        logging.info("Estrutura do banco de dados criada/verificada com sucesso.")
        if root:
            messagebox.showinfo("Sucesso", "Estrutura do banco de dados verificada/criada com sucesso!")
        
    except FileNotFoundError as e:
        logging.error(f"Arquivo de script SQL não encontrado: {e}")
        if root: messagebox.showerror("Erro de Arquivo", f"Não foi possível encontrar um arquivo de script necessário:\n{e}")
        else: raise e
            
    except Exception as e:
        logging.error(f"Falha crítica ao configurar o banco de dados: {e}")
        if conn: conn.rollback() # Garante que nada parcial seja salvo
        if root: messagebox.showerror("Erro ao Configurar Banco de Dados", f"Ocorreu um erro crítico:\n{e}")
        else: raise e
    finally:
        if conn:
            cur.close()
            conn.close()
