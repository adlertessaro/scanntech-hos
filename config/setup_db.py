from db.conexao import conectar
from tkinter import messagebox
from pathlib import Path
import sys
import os
import logging
import psycopg2.errors

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
    Executa scripts SQL garantindo que falhas em objetos existentes não 
    travem a execução dos próximos comandos.
    """
    with open(path, "r", encoding="utf-8") as file:
        # Removemos comentários para não confundir o fatiamento por ';'
        linhas = file.readlines()
        script_limpo = "".join([l for l in linhas if not l.strip().startswith("--")])

    # Triggers precisam rodar como um bloco único. Tabelas podem ser fatiadas.
    comandos = [script_limpo] if is_trigger_script else script_limpo.split(';')

    for sql in comandos:
        comando = sql.strip()
        if not comando:
            continue
            
        try:
            # Criamos um SAVEPOINT. É como um "check-point" de videogame.
            # Se a gente morrer no comando, voltamos para cá sem perder o jogo todo.
            logging.info(f"Tentando executar: {comando[:100]}...")
            cur.execute("SAVEPOINT comando_atual;")
            cur.execute(comando)
            cur.execute("RELEASE SAVEPOINT comando_atual;")
            logging.info(f"Sucesso no comando: {comando[:50]}...")
        except (psycopg2.errors.DuplicateTable, psycopg2.errors.DuplicateColumn, 
                psycopg2.errors.DuplicateObject, psycopg2.errors.DuplicateFunction):
            # Se o erro for apenas "isso já existe", voltamos ao checkpoint e ignoramos.
            cur.execute("ROLLBACK TO SAVEPOINT comando_atual;")
            logging.warning(f"Objeto/Coluna já existente, pulando: {comando[:50]}...")
        except Exception as e:
            # Se for um erro de verdade (sintaxe ou conexão), aí sim paramos tudo.
            cur.execute("ROLLBACK TO SAVEPOINT comando_atual;")
            logging.error(f"Erro real ao executar: {e}")
            raise e

    # Ao final de todos os comandos do arquivo, enviamos para o banco de vez.
    conn.commit()
    logging.info(f"Script {path.name} finalizado.")

# def aplicar_migracoes_promocoes(cur, conn):
#     """
#     Garante que a tabela 'promocao_cab' tenha a coluna 'id_scanntech' e a restrição UNIQUE.
#     """
#     logging.info("Verificando e aplicando migrações para a tabela 'promocao_cab'...")
#     try:
#         cur.execute("ALTER TABLE promocao_cab ADD COLUMN id_scanntech VARCHAR(255);")
#         logging.info("Coluna 'id_scanntech' adicionada com sucesso em 'promocao_cab'.")
#     except psycopg2.errors.DuplicateColumn:
#         logging.info("Coluna 'id_scanntech' já existe em 'promocao_cab'. Nenhuma ação necessária.")
#         conn.rollback() # Importante para continuar a execução
    
#     try:
#         cur.execute("ALTER TABLE promocao_cab ADD CONSTRAINT uk_promocao_cab_id_scanntech UNIQUE (id_scanntech);")
#         logging.info("Restrição UNIQUE para 'id_scanntech' adicionada com sucesso.")
#     except psycopg2.errors.DuplicateTable: # Erro para constraint já existente é DuplicateTable
#         logging.info("Restrição UNIQUE para 'id_scanntech' já existe. Nenhuma ação necessária.")
#         conn.rollback() # Importante para continuar a execução

def criar_tabelas_scanntech(root=None):
    """
    Conecta ao banco, cria as tabelas, aplica migrações e depois cria as triggers.
    Gerencia a transação manualmente para maior controle.
    """
    conn = None
    try:
        conn = conectar()
        cur = conn.cursor()

        # cur.execute("SET lock_timeout = '5s';")
        
        # script_tabelas = resource_path("db/create_tables.sql")
        script_tabelas_vendas = resource_path("db/create_tables_vendas.sql")
        script_tabelas_fechamentos = resource_path("db/create_tables_fechamentos.sql")
        script_tabelas_promo = resource_path("db/create_tables_promo.sql")
        script_triggers = resource_path("db/create_triggers.sql")

        logging.info(f"Executando script de criação de tabelas de integração: {script_tabelas_vendas}")
        executar_script(script_tabelas_vendas, cur, conn)
        logging.info(f"Executando script de criação de tabelas de integração: {script_tabelas_fechamentos}")
        executar_script(script_tabelas_fechamentos, cur, conn)
        logging.info(f"Executando script de criação de tabelas de integração: {script_tabelas_promo}")
        executar_script(script_tabelas_promo, cur, conn)

        # aplicar_migracoes_promocoes(cur, conn)

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
