# import duckdb
# from datetime import datetime, timedelta
# from pathlib import Path

# # Caminho do banco DuckDB
# DB_PATH = str(Path(__file__).parent.parent / "controle.duckdb")

# # Inicializa a tabela, se necessário
# def inicializar_db():
#     with duckdb.connect(DB_PATH) as conn:
#         conn.execute("""
#             CREATE TABLE IF NOT EXISTS execucoes_integracao (
#                 rotina TEXT PRIMARY KEY,
#                 ultima_execucao TIMESTAMP
#             );
#         """)

# # Verifica se já passou o intervalo desde a última execução
# def deve_executar(rotina: str, intervalo_minutos: int) -> bool:
#     inicializar_db()
#     with duckdb.connect(DB_PATH) as conn:
#         result = conn.execute(
#             "SELECT ultima_execucao FROM execucoes_integracao WHERE rotina = ?",
#             [rotina]
#         ).fetchone()

#         if not result or not result[0]:
#             return True  # nunca executado

#         ultima_execucao = result[0]
#         proxima_execucao = ultima_execucao + timedelta(minutes=intervalo_minutos)
#         return datetime.now() >= proxima_execucao

# # Atualiza a data/hora da última execução
# def atualizar_execucao(rotina: str):
#     inicializar_db()
#     with duckdb.connect(DB_PATH) as conn:
#         conn.execute("""
#             INSERT INTO execucoes_integracao (rotina, ultima_execucao)
#             VALUES (?, ?)
#             ON CONFLICT (rotina) DO UPDATE SET ultima_execucao = excluded.ultima_execucao;
#         """, [rotina, datetime.now()])
