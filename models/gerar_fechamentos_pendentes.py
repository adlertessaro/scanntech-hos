from datetime import datetime, timedelta
from scanntech.db.conexao import conectar

def gerar_fechamentos_pendentes(config, dias_retroativos=7):
    print("🔍 Chaves carregadas do config (debug real):", config.keys())

    try:
        empresa = config.get("empresa")
        if not empresa:
            print("⚠️ Nenhuma empresa configurada no campo 'empresa'. Encerrando geração de fechamentos.")
            return

        hoje = datetime.now().date()
        data_inicio = hoje - timedelta(days=dias_retroativos)

        conn = conectar()
        cur = conn.cursor()

        data_atual = data_inicio
        while data_atual < hoje:
            cur.execute("""
                SELECT 1 FROM int_scanntech_fechamentos
                WHERE empresa = %s AND data_fechamento = %s
            """, (empresa, data_atual))

            if cur.fetchone() is None:
                cur.execute("""
                    INSERT INTO int_scanntech_fechamentos (
                        data_fechamento,
                        empresa,
                        tentativas,
                        data_hora_inclusao
                    ) VALUES (%s, %s, %s, %s)
                """, (data_atual, empresa, 0, datetime.now()))
                print(f"✅ Fechamento gerado para {data_atual} (Empresa {empresa})")
            else:
                print(f"🔁 Já existe fechamento para {data_atual} (Empresa {empresa})")

            data_atual += timedelta(days=1)

        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Geração de fechamentos pendentes finalizada para empresa {empresa} (últimos {dias_retroativos} dias).\n")

    except Exception as e:
        print(f"❌ Erro ao gerar fechamentos pendentes: {e}")