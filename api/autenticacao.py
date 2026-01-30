# scanntech/api/autenticacao.py

import time
import logging
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import pytz
import unicodedata
from urllib.parse import urljoin

try:
    from scanntech.utils.versao import obter_versao_exe
except ImportError:
    from ..utils.versao import obter_versao_exe


# ---------------------------
# Utilitários existentes
# ---------------------------

def montar_autenticacao(usuario, senha):
    return HTTPBasicAuth(usuario, senha)

def corrigir_payload_vendas(vendas):
    def remove_acentos(texto):
        return ''.join(c for c in unicodedata.normalize('NFD', texto)
                       if unicodedata.category(c) != 'Mn')

    for venda in vendas:
        if 'fecha' in venda and venda['fecha']:
            try:
                dt = datetime.fromisoformat(venda['fecha'].replace('Z', '+00:00'))
                dt = dt.astimezone(pytz.timezone('America/Sao_Paulo'))
                venda['fecha'] = dt.strftime('%Y-%m-%dT%H:%M:%S-03:00')
            except ValueError:
                logging.warning(
                    f"⚠️ Formato inválido de 'fecha' na venda {venda.get('numero')}: {venda['fecha']}"
                )

        if 'cotizacion' in venda:
            venda['cotizacion'] = (
                float(venda['cotizacion']) if isinstance(venda['cotizacion'], str)
                else venda['cotizacion']
            )

        for pagamento in venda.get('pagos', []):
            if 'cotizacion' in pagamento:
                pagamento['cotizacion'] = (
                    float(pagamento['cotizacion']) if isinstance(pagamento['cotizacion'], str)
                    else pagamento['cotizacion']
                )

        for detalhe in venda.get('detalles', []):
            if 'descripcionArticulo' in detalhe and detalhe['descripcionArticulo']:
                detalhe['descripcionArticulo'] = remove_acentos(detalhe['descripcionArticulo'])

    return vendas


# ---------------------------
# Versões do backend/PDV
# ---------------------------

try:
    caminho_backend = r"C:\MercFarma\HOSFarmaV8\HOSFarma.exe"
    caminho_pdv = r"C:\MercFarma\FcNovo\HOSFrenteDeCaixa.exe"
    
    versao_backend = obter_versao_exe(caminho_backend)
    versao_pdv = obter_versao_exe(caminho_pdv)
    
    # LOG DE DIAGNÓSTICO
    logging.info(f"✅ Versões carregadas com sucesso:")
    logging.info(f"   PDV: {versao_pdv} (de {caminho_pdv})")
    logging.info(f"   Backend: {versao_backend} (de {caminho_backend})")
    
except Exception as e:
    logging.error(f"❌ Erro ao obter versões dos executáveis: {e}")
    logging.error(f"   Caminho PDV: {caminho_pdv}")
    logging.error(f"   Caminho Backend: {caminho_backend}")
    
    # 🔥 FALLBACK: Tenta versão genérica
    versao_backend = "1.0.0"
    versao_pdv = "1.0.0"
    
    logging.warning(f"⚠️  Usando versões fallback: PDV={versao_pdv}, Backend={versao_backend}")

BASE_HEADERS = {
    "pdv-version": versao_pdv,
    "backEnd-version": versao_backend,
}

# 🔥 LOG DOS HEADERS MONTADOS
logging.info(f"📤 Headers BASE configurados: {BASE_HEADERS}")

# ---------------------------
# Nova rotina com retry e timers
# ---------------------------

def _join_url(url_base: str, endpoint_path: str) -> str:
    """
    Une base + endpoint com uma única barra entre eles.
    Evita 'http://x.comapi...' ou 'http://x.com//api...'.
    """
    url_base = (url_base or "").strip()
    endpoint_path = (endpoint_path or "").strip()
    if not url_base:
        return endpoint_path
    # Garante uma barra final para o urljoin funcionar como esperado
    if not url_base.endswith("/"):
        url_base += "/"
    return urljoin(url_base, endpoint_path.lstrip("/"))


def fazer_requisicao(config, endpoint_path, metodo='GET', dados=None, headers=None, 
                     retries_por_url=3, timeout_segundos=30, intervalo_entre_urls=2.0):
    """
    Executa uma requisição HTTP para a API da Scanntech com fallback entre múltiplas URLs.
    Versões do PDV e Backend são protegidas e não podem ser sobrescritas.
    """
    logging.debug(f"Iniciando fazer_requisicao para endpoint: {endpoint_path}")

    # 🔥 DIAGNÓSTICO: Ver o que tem no config
    logging.debug(f"🔍 Config recebido: {config}")
    logging.debug(f"🔍 Tipo do config: {type(config)}")
    logging.debug(f"🔍 Keys do config: {list(config.keys()) if isinstance(config, dict) else 'N/A'}")
    
    # 🔥 DEFINIR VARIÁVEIS FALTANTES
    backoff_segundos = [1.0, 2.0, 4.0]  # Tempos de espera progressivos
    
    # Obter URLs e credenciais do config
    urls = []
    for i in range(1, 4):
        url = config.get(f"url{i}") or config.get(f"url_{i}", "")  # Tenta ambos os formatos
        if url and url.strip():
            urls.append(url.strip())
    usuario = config.get("usuario", "").strip()
    senha = config.get("senha", "").strip()
    
    if not urls:
        return {"status_code": None, "sucesso": False, "mensagem": "Nenhuma URL configurada."}
    if not usuario or not senha:
        return {"status_code": None, "sucesso": False, "mensagem": "Usuário ou senha não configurados."}
    
    auth = montar_autenticacao(usuario, senha)
    
    # 🔥 MONTAGEM PROTEGIDA DE HEADERS
    final_headers = dict(BASE_HEADERS)
    
    if metodo.upper() == 'POST':
        final_headers['Content-Type'] = 'application/json'
    
    if headers:
        headers_filtrados = {
            k: v for k, v in headers.items() 
            if k not in ['pdv-version', 'backEnd-version']
        }
        final_headers.update(headers_filtrados)
        
        if 'pdv-version' in headers or 'backEnd-version' in headers:
            logging.warning(f"⚠️ Tentativa de sobrescrever versões bloqueada. Mantendo: pdv-version={BASE_HEADERS['pdv-version']}, backEnd-version={BASE_HEADERS['backEnd-version']}")
    
    logging.debug(f"Headers finais da requisição: {final_headers}")
    
    # Tentar cada URL configurada
    for url_base in filter(None, urls):
        url_final = _join_url(url_base, endpoint_path)
        logging.debug(f"Tentando host: {url_base} → endpoint: {endpoint_path} → URL final: {url_final}")
        
        # RETRY POR HOST
        for tentativa in range(1, retries_por_url + 1):
            try:
                resposta = requests.request(
                    method=metodo.upper(),
                    url=url_final,
                    auth=auth,
                    json=dados,
                    headers=final_headers,
                    timeout=timeout_segundos,
                )
                
                logging.debug(f"Resposta recebida de {url_final} com status: {resposta.status_code}")
                
                # Tenta decodificar JSON
                try:
                    dados_resposta = resposta.json()
                except requests.exceptions.JSONDecodeError:
                    dados_resposta = resposta.text
                
                # 2xx → sucesso
                if resposta.ok:
                    return {"status_code": resposta.status_code, "sucesso": True, "dados": dados_resposta}
                
                # 5xx → erro no servidor: retry
                if 500 <= resposta.status_code < 600:
                    logging.warning(f"Erro {resposta.status_code} em {url_final} (tentativa {tentativa}/{retries_por_url})")
                    
                    if tentativa < retries_por_url:
                        atraso = backoff_segundos[min(tentativa - 1, len(backoff_segundos) - 1)]
                        logging.info(f"⏳ Aguardando {atraso:.1f}s para nova tentativa...")
                        time.sleep(atraso)
                        continue
                    else:
                        break
                
                # 4xx → erro do cliente: não retry
                logging.error(f"Erro HTTP {resposta.status_code} em {url_final}: {resposta.text}")
                return {
                    "status_code": resposta.status_code,
                    "sucesso": False,
                    "mensagem": resposta.text,
                    "dados": dados_resposta
                }
                
            except requests.exceptions.Timeout as e:
                logging.warning(f"Timeout ao acessar {url_final} (tentativa {tentativa}/{retries_por_url}): {e}")
                
                if tentativa < retries_por_url:
                    atraso = backoff_segundos[min(tentativa - 1, len(backoff_segundos) - 1)]
                    time.sleep(atraso)
                    continue
                else:
                    break
                    
            except requests.exceptions.RequestException as e:
                logging.warning(f"Erro de conexão ao acessar {url_final} (tentativa {tentativa}/{retries_por_url}): {e}")
                
                if tentativa < retries_por_url:
                    atraso = backoff_segundos[min(tentativa - 1, len(backoff_segundos) - 1)]
                    time.sleep(atraso)
                    continue
                else:
                    break
        
        # Intervalo entre hosts
        logging.info(f"⏳ Aguardando {intervalo_entre_urls:.1f}s antes do próximo host...")
        time.sleep(intervalo_entre_urls)
    
    logging.error("Todas as URLs configuradas falharam.")
    return {
        "status_code": 503,
        "sucesso": False,
        "mensagem": "Falha ao conectar em todas as URLs configuradas."
    }

