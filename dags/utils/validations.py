"""
Validações de integridade pós-carga.
"""

import csv
import logging

import psycopg2
from psycopg2 import sql

from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook

from utils.config import TABLE_SOURCE_MAP

logger = logging.getLogger(__name__)

def _count_csv_rows(path: str) -> int:
    with open(path, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1


def _count_destination_rows(conn, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {};").format(sql.Identifier(schema, table))
        )
        return cur.fetchone()[0]


def validate_row_counts(**context) -> None:
    """
    Callable do PythonOperator `validate_row_counts_task`.

    Lança AirflowFailException se qualquer tabela tiver divergência de
    contagem entre origem e destino, garantindo que a DAG só seja
    marcada como sucesso quando os dados realmente chegaram íntegros.
    """
    conn_info = BaseHook.get_connection("banvic_dest")
    conn = psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port,
        user=conn_info.login,
        password=conn_info.password,
        dbname=conn_info.schema,
    )

    mismatches = []
    try:
        for table, csv_path in TABLE_SOURCE_MAP.items():
            source_count = _count_csv_rows(csv_path)
            dest_count = _count_destination_rows(conn, "raw_banvic", table)

            logger.info(
                "Tabela %s: origem=%d destino=%d", table, source_count, dest_count
            )

            if source_count != dest_count:
                mismatches.append(
                    f"{table}: origem={source_count} destino={dest_count}"
                )
    finally:
        conn.close()

    if mismatches:
        raise AirflowFailException(
            "Divergência de contagem entre origem e destino:\n"
            + "\n".join(mismatches)
        )
