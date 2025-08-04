from scanntech.api.scanntech_api_promocoes import consultar_promocoes as get_promocoes
from scanntech.db.promo_repo import salvar_promocoes, reprocessar_produtos_pendentes

def processar_promocoes(config):
    respostas = [
        get_promocoes(config)
        # get_promocoes_limite_ticket(config),
        # get_promocoes_crm(config)
    ]

    total_geral = 0

    for resposta in respostas:
        if not isinstance(resposta, dict):
            print("❌ Erro: resposta não é um dicionário.")
            continue

        status = resposta.get("status_code", 0)
        if status != 200:
            print(f"❌ Erro ao consultar promoções. Código: {status}")
            continue

        dados = resposta.get("dados", {})
        total = dados.get("total", 0)
        resultados = dados.get("results", [])

        if total > 0:
            salvar_promocoes(resultados, config)
            total_geral += total
            print(f"✅ {total} promoções salvas com sucesso.")
        else:
            print("🔍 Nenhuma promoção ativa neste endpoint.")

    if total_geral == 0:
        print("📭 Nenhuma promoção encontrada em nenhum dos endpoints.")
    else:
        print("🔄 Iniciando reprocessamento de produtos pendentes...")
        reprocessar_produtos_pendentes()  # Chama a função após salvar