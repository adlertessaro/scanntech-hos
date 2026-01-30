import os
import psycopg2
import json
import base64
from pathlib import Path
from Crypto.Cipher import AES
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Util.Padding import unpad

def descriptografar_senha_hos(cipher_text_b64):
    salt = b"hossalt23214400"
    password = "HOS23214400"
    encrypted_data = base64.b64decode(cipher_text_b64)
    key_iv = PBKDF2(password, salt, dkLen=32 + 16)
    key = key_iv[:32]
    iv = key_iv[32:]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = cipher.decrypt(encrypted_data)
    return unpad(decrypted, AES.block_size).decode("utf-8")

def obter_dados_conexao():
    # Caminho do arquivo .hos/config.config no diretório do usuário
    caminho_config = Path.home() / ".hos" / "config"

    if not os.path.exists(caminho_config):
        raise FileNotFoundError("Arquivo de configuração do banco não encontrado.")

    with open(caminho_config, "r", encoding="utf-8") as f:
        dados = json.load(f)

    config = dados.get("Configuracoes", {})

    if config.get("BANCO_DADOS", "").upper() != "POSTGRES":
        raise ValueError("Somente disponível para POSTGRES.")

    return {
        'host': config.get('IP_REMOTO', 'localhost'),
        'port': 5432,
        'user': config.get('USERBANCO'),
        'password': descriptografar_senha_hos(config.get('SENHABANCO')),
        'dbname': config.get('NOME_BANCODADOS')
    }

def conectar():
    dados = obter_dados_conexao()
    return psycopg2.connect(**dados)
