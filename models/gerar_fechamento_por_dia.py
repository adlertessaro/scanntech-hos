
# from datetime import datetime
# import duckdb
# from pathlib import Path

# DB_PATH = r"/home/sandbox/controle.duckdb"

# def gerar_fechamento_por_dia(config, data):
#     empresa = config.get("id_empresa") or config.get("empresa")
#     if not empresa:
#         print("❌ Empresa não definida no config.")
#         return

#     data_fechamento = data.isoformat()

#     with duckdb.connect(DB_PATH) as conn:
#         # Verifica se já existe
#         existe = conn.execute(
#             "SELECT COUNT(*) FROM int_scanntech_fechamentos WHERE data_fechamento = ? AND empresa = ?",
#             [data_fechamento, float(empresa)]
#         ).fetchone()[0]

#         if existe > 0:
#             print(f"⚠️ Fechamento já existe para {data.strftime('%d/%m/%Y')}")
#             return

#         # Insere o registro pendente
#         conn.execute("""
#             INSERT INTO int_scanntech_fechamentos (
#                 data_fechamento,
#                 empresa,
#                 tentativas,
#                 data_hora_inclusao,
#                 data_hora_tentativa,
#                 id_lote,
#                 erro
#             )
#             VALUES (?, ?, ?, ?, ?, NULL, NULL)
#         """, [
#             data_fechamento,
#             float(empresa),
#             0,
#             datetime.now(),
#             None
#         ])

#         print(f"✅ Fechamento pendente registrado para {data.strftime('%d/%m/%Y')}")
