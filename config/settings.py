# settings.py

from configparser import ConfigParser
from pathlib import Path
import os
import sys
import logging

# --- LÓGICA DE CAMINHO CORRIGIDA PARA A RESTRIÇÃO ATUAL ---

# Garante que o caminho base seja sempre a pasta onde este arquivo (settings.py) está.
# Isso força o programa a procurar 'settings.config' na mesma pasta.
if getattr(sys, 'frozen', False):  # Se estiver rodando como .exe
    # Assume que o .config estará junto ao .exe
    BASE_DIR = Path(sys.executable).parent
else:  # Se estiver rodando como script Python
    # Path(__file__).parent aponta para a pasta do arquivo atual (.../scanntech/config/)
    BASE_DIR = Path(__file__).parent

# Define o caminho único e correto para o arquivo de configuração, respeitando a restrição.
CONFIG_PATH = BASE_DIR / "settings.config"

# --- FIM DA LÓGICA DE CAMINHO ---

CHAVE_CRIPTO = b'YsbSwFbAnHR0z2dGRWkmXsh5SxUlWzF6RDbAvmt0_AA='
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

def carregar_configuracoes():
    """
    Carrega as configurações do arquivo settings.config de forma robusta.
    """
    logging.debug(f"Tentando carregar configurações de: {CONFIG_PATH}")
    if not os.path.exists(CONFIG_PATH):
        # Se o arquivo não existe, cria um vazio para evitar erro na primeira execução
        logging.warning(f"Arquivo de configuração não encontrado em {CONFIG_PATH}. Um novo será criado ao salvar.")
        return {} # Retorna um dicionário vazio

    parser = ConfigParser()
    parser.read(CONFIG_PATH, encoding="utf-8")

    if "SCANNTECH" not in parser:
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                content = f.read()
                if content and not content.strip().startswith('['):
                    logging.warning("Arquivo de configuração parece ser antigo (sem header). Adicionando [SCANNTECH] para leitura.")
                    parser.read_string(f"[SCANNTECH]\n{content}")
        except Exception as e:
            logging.error(f"Erro ao tentar ler arquivo em modo de fallback: {e}")
            return {} # Retorna vazio em caso de erro

    if "SCANNTECH" not in parser:
        logging.error(f"Seção [SCANNTECH] não encontrada no arquivo: {CONFIG_PATH}")
        return {}

    config = {k.lower(): v for k, v in parser["SCANNTECH"].items()}
    logging.debug("Configurações carregadas com sucesso.")
    return config

def exibir_configuracoes(config_dict):
    """
    Exibe de forma legível as configurações contidas em um dicionário.
    """
    print("\n🛠️  Configurações Atuais Carregadas:\n")
    if not config_dict:
        print("Nenhuma configuração para exibir.")
        return
        
    for chave, valor in config_dict.items():
        if 'senha' in chave:
            valor = '********'
        print(f"🔹 {chave}: {valor}")