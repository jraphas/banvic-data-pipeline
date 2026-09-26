"""
Configuração central dos dados de origem.
"""

SOURCE_DATA_DIR = "/opt/airflow/data"

TABLE_SOURCE_MAP = {
    "agencias": f"{SOURCE_DATA_DIR}/agencias.csv",
    "clientes": f"{SOURCE_DATA_DIR}/clientes.csv",
    "colaborador_agencia": f"{SOURCE_DATA_DIR}/colaborador_agencia.csv",
    "colaboradores": f"{SOURCE_DATA_DIR}/colaboradores.csv",
    "contas": f"{SOURCE_DATA_DIR}/contas.csv",
    "propostas_credito": f"{SOURCE_DATA_DIR}/propostas_credito.csv",
    "transacoes": f"{SOURCE_DATA_DIR}/transacoes.csv",
}
