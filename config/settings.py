# settings.py

from configparser import ConfigParser
from pathlib import Path
import os
import sys
import logging
import unicodedata

# --- LÓGICA DE CAMINHO (sem alterações) ---
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "settings.config"
# --- FIM DA LÓGICA DE CAMINHO ---

CHAVE_CRIPTO = b'YsbSwFbAnHR0z2dGRWkmXsh5SxUlWzF6RDbAvmt0_AA='
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# --- NOVA FUNÇÃO INTERNA PARA TRATAR NOMES DE CHAVES ---
def _normalizar_chave(key_string):
    """Normaliza uma chave: remove acentos, espaços e converte para minúsculas."""
    s = ''.join(c for c in unicodedata.normalize('NFD', key_string) if unicodedata.category(c) != 'Mn')
    return s.lower().replace(' ', '_').replace('(', '').replace(')', '')

def carregar_configuracoes():
    """
    Carrega as configurações do arquivo settings.config, sendo compatível com o formato
    antigo (uma única seção [SCANNTECH]) e o novo (múltiplas seções).
    """
    logging.debug(f"Tentando carregar configurações de: {CONFIG_PATH}")
    if not os.path.exists(CONFIG_PATH):
        logging.warning(f"Arquivo de configuração não encontrado em {CONFIG_PATH}.")
        return {"geral": {}, "lojas": []}

    parser = ConfigParser()
    parser.read(CONFIG_PATH, encoding="utf-8")

    # Se o novo formato existir, use-o
    if parser.has_section("SCANNTECH_GERAL"):
        logging.info("Formato de configuração novo detectado ([SCANNTECH_GERAL]).")
        config_geral = {_normalizar_chave(k): v for k, v in parser["SCANNTECH_GERAL"].items()}
        lojas = []
        for section in parser.sections():
            if section.upper().startswith("LOJA_"):
                loja_config = {_normalizar_chave(k): v for k, v in parser[section].items()}
                lojas.append(loja_config)
        return {"geral": config_geral, "lojas": lojas}
    
    # Se não, tente carregar o formato antigo
    elif parser.has_section("SCANNTECH"):
        logging.warning("Formato de configuração antigo detectado ([SCANNTECH]). Convertendo para o novo formato em memória.")
        config_antiga = {_normalizar_chave(k): v for k, v in parser["SCANNTECH"].items()}
        
        # Define quais chaves pertencem à configuração geral
        chaves_gerais_set = {
            "habilitar_integracao_scanntech", "usuario", "senha", "url_1", "url_2", "url_3", 
            "intervalo_s", "data_de_inicio", "data_inicio_envio_de_fechamentos", "data_inicio_envio_de_vendas"
        }

        config_geral = {}
        loja_config = {}

        for k, v in config_antiga.items():
            if k in chaves_gerais_set:
                config_geral[k] = v
            else:
                # Mapeia nomes antigos para os novos nomes de chaves de loja
                if k == "codigo_da_empresa":
                    loja_config["idempresa"] = v
                elif k == "local":
                    loja_config["idlocal"] = v
                else: # Mantém outras chaves como 'empresa', 'crm'
                    loja_config[k] = v

        return {"geral": config_geral, "lojas": [loja_config] if loja_config else []}
    
    else:
        logging.error(f"Nenhuma seção válida ([SCANNTECH_GERAL] ou [SCANNTECH]) encontrada em {CONFIG_PATH}")
        return {"geral": {}, "lojas": []}


def exibir_configuracoes(config_dict):
    # (sem alterações nesta função)
    print("\n🛠️  Configurações Atuais Carregadas:\n")
    if not config_dict or not config_dict.get('geral'):
        print("Nenhuma configuração para exibir.")
        return
    
    print("--- Configurações Gerais ---")
    for chave, valor in config_dict['geral'].items():
        if 'senha' in chave:
            valor = '********'
        print(f"🔹 {chave}: {valor}")
    
    print("\n--- Lojas Configuradas ---")
    if not config_dict.get('lojas'):
        print("Nenhuma loja configurada.")
        return

    for i, loja in enumerate(config_dict['lojas'], 1):
        print(f"\n[ Loja {i} ]")
        for chave, valor in loja.items():
            print(f"  🔸 {chave}: {valor}")