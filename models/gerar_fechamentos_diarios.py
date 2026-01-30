from datetime import datetime, timedelta
from scanntech.db.conexao import conectar

def gerar_fechamentos_pendentes(config):
    print("🔍 Chaves carregadas do config (debug real):", config.keys())

    try:
        empresa = config.get("empresa")
        if not empresa:
            print("⚠️ Nenhuma empresa configurada no campo 'empresa'. Encerrando geração de fechamentos.")
            return

        data_inicio_str = config.get("data início envio de fechamentos")
        if not data_inicio_str:
            print("⚠️ Nenhuma data de início para fechamentos foi configurada.")
            return

        data_inicio = datetime.strptime(data_inicio_str, "%d/%m/%Y").date()
        hoje = datetime.now().date()

        conn = conectar()
        cur = conn.cursor()

        data_atual = data_inicio
        while data_atual < hoje:
            # --- ALTERAÇÃO APLICADA AQUI ---
            # Busca estações com movimentação na data
            cur.execute(
                """
                SELECT DISTINCT estacao FROM caixa
                WHERE empresa = %s
                  AND data = %s
                  AND lancamen IN ('VV', 'VP', 'VC', 'VR', 'CC', 'DV')
                """,
                (empresa, data_atual),
            )
            estacoes_movimento = [row[0] for row in cur.fetchall()]

            if not estacoes_movimento:
                print(f"⚠️ Sem movimentação em {data_atual} (Empresa {empresa}).")
            else:
                for estacao in estacoes_movimento:
                    # Verifica a existência do fechamento para a estação
                    cur.execute(
                        """
                        SELECT 1 FROM int_scanntech_fechamentos
                        WHERE empresa = %s AND data_fechamento = %s AND estacao = %s
                        """,
                        (empresa, data_atual, estacao),
                    )

                    if cur.fetchone() is None:
                        # Insere o fechamento pendente para a estação
                        cur.execute(
                            """
                            INSERT INTO int_scanntech_fechamentos (
                                data_fechamento,
                                empresa,
                                estacao,
                                tentativas,
                                data_hora_inclusao
                            ) VALUES (%s, %s, %s, %s, %s)
                            """,
                            (data_atual, empresa, estacao, 0, datetime.now()),
                        )
                        print(f"✅ Fechamento gerado para {data_atual} (Empresa {empresa}, Estação {estacao})")
                    else:
                        print(f"🔁 Já existe fechamento para {data_atual} (Empresa {empresa}, Estação {estacao})")

            data_atual += timedelta(days=1)

        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Geração de fechamentos pendentes finalizada para empresa {empresa}.\n")

    except Exception as e:
        print(f"❌ Erro ao gerar fechamentos pendentes: {e}")