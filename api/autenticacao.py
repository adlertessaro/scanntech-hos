import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import pytz
from utils.versao import obter_versao_exe

def montar_autenticacao(usuario, senha):
    """
    Retorna um objeto de autenticação básica para usar nas requisições.
    """
    return HTTPBasicAuth(usuario, senha)

def corrigir_payload_vendas(vendas):
    """
    Corrige o payload para atender às validações da API:
    - Adiciona fuso horário em 'fecha'.
    - Converte 'cotizacion' para número.
    - Remove caracteres acentuados em 'descripcionArticulo'.
    """
    def remove_acentos(texto):
        import unicodedata
        return ''.join(c for c in unicodedata.normalize('NFD', texto)
                       if unicodedata.category(c) != 'Mn')

    for venda in vendas:
        # Adiciona fuso horário (-03:00) em 'fecha'
        if 'fecha' in venda and venda['fecha']:
            try:
                dt = datetime.fromisoformat(venda['fecha'].replace('Z', '+00:00'))
                dt = dt.astimezone(pytz.timezone('America/Sao_Paulo'))
                venda['fecha'] = dt.strftime('%Y-%m-%dT%H:%M:%S-03:00')
            except ValueError:
                print(f"⚠️ Formato inválido de 'fecha' na venda {venda.get('numero')}: {venda['fecha']}")

        # Converte 'cotizacion' para número
        if 'cotizacion' in venda:
            venda['cotizacion'] = float(venda['cotizacion']) if isinstance(venda['cotizacion'], str) else venda['cotizacion']
        for pagamento in venda.get('pagos', []):
            if 'cotizacion' in pagamento:
                pagamento['cotizacion'] = float(pagamento['cotizacion']) if isinstance(pagamento['cotizacion'], str) else pagamento['cotizacion']

        # Remove acentos em 'descripcionArticulo'
        for detalhe in venda.get('detalles', []):
            if 'descripcionArticulo' in detalhe and detalhe['descripcionArticulo']:
                detalhe['descripcionArticulo'] = remove_acentos(detalhe['descripcionArticulo'])

    return vendas

# Caminhos reais dos executáveis
caminho_backend = r"C:\MercFarma\HOSFarmaV8\HOSFarma.exe"
caminho_pdv = r"C:\MercFarma\FcNovo\HOSFrenteDeCaixa.exe"

# Obtendo as versões reais
versao_backend = obter_versao_exe(caminho_backend)
versao_pdv = obter_versao_exe(caminho_pdv)

# 2. Cria um dicionário base de cabeçalhos
BASE_HEADERS = {
    "pdv-version": versao_pdv,
    "backEnd-version": versao_backend
}

def fazer_requisicao(config, endpoint_path, metodo='GET', dados=None, params=None, headers=None):
    urls = [config.get('url 1'), config.get('url 2'), config.get('url 3')]
    id_empresa = config.get('código da empresa')
    id_local = config.get('local')
    id_caja = config.get('caja', '1')

    if not id_empresa or not id_local:
        return {
            "erro": "Parâmetros inválidos",
            "mensagem": f"id_empresa ({id_empresa}) ou id_local ({id_local}) não fornecidos."
        }

    auth = montar_autenticacao(config['usuário'], config['senha'])

    # 3. Constroi os cabeçalhos finais aqui
    final_headers = BASE_HEADERS.copy()
    if metodo.upper() == 'POST':
        final_headers['Content-Type'] = 'application/json'
    
    # Permite que chamadas específicas adicionem ou substituam cabeçalhos
    if headers:
        final_headers.update(headers)


    for url_base in urls:
        if not url_base:
            continue

        endpoint = endpoint_path.format(idEmpresa=id_empresa, idLocal=id_local)
        url_final = f"{url_base}{endpoint}"
        print(f"🔗 URL final montada: {url_final}")
        
        # DEBUG
        print(f"DEBUG: 📋 Cabeçalhos que serão enviados: {final_headers}")

        try:
            if metodo.upper() == 'GET':
                resposta = requests.get(url_final, auth=auth, params=params, headers=final_headers, timeout=10)
            elif metodo.upper() == 'POST':
                resposta = requests.post(url_final, auth=auth, json=dados, headers=final_headers, timeout=10)
            else:
                raise ValueError(f"Método HTTP não suportado: {metodo}")

            print("🔍 Status code:", resposta.status_code)
            print("📦 Resposta:", resposta.text)

            try:
                dados_resposta = resposta.json()
            except ValueError:
                dados_resposta = resposta.text

            numero = dados[0].get("numero") if isinstance(dados, list) and dados else "consulta"
            tipo_envio = 'venda' if '/movimientos/lotes' in endpoint_path else (
                'fechamento' if '/cierresDiarios/lotes' in endpoint_path else 'promocao'
            )

            if resposta.status_code == 400:
                print(f"❌ Erro HTTP ao enviar {tipo_envio} {numero}: HTTP 400 – {resposta.text}")

            if resposta.status_code == 200:
                return {
                    "status": "ok",
                    "status_code": 200,
                    "dados": dados_resposta
                }
            else:
                return {
                    "status": "erro",
                    "status_code": resposta.status_code,
                    "mensagem": resposta.text,
                    "dados": dados_resposta
                }

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Erro ao acessar {url_base}: {e}")
            continue

    return {
        "erro": "Nenhuma URL disponível",
        "mensagem": "Falha ao conectar em todas as URLs configuradas."
    }

#APAGAR
# def enviar_vendas_lote(config, id_caja, vendas):
#     id_caja = int(id_caja)  # força inteiro

#     # Corrigir o payload antes de enviar
#     vendas_corrigidas = corrigir_payload_vendas(vendas)

#     # 🔍 Print do payload formatado
#     print("📤 Payload que será enviado:")
#     print(json.dumps(vendas_corrigidas, indent=2, ensure_ascii=False))

#     endpoint_path = f"/api-minoristas/api/v2/minoristas/{{idEmpresa}}/locales/{{idLocal}}/cajas/{id_caja}/movimientos/lotes"

#     resultado = fazer_requisicao(config, endpoint_path, metodo='POST', dados=vendas_corrigidas)

#     return resultado