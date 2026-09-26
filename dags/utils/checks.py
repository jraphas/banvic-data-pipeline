"""
Checagem de disponibilidade dos arquivos de origem antes da extração.
"""

import logging
import os

from utils.config import TABLE_SOURCE_MAP

logger = logging.getLogger(__name__)

def all_source_files_exist(**context) -> bool:
    """
    Callable do PythonSensor `check_source_files`.

    Retorna True somente quando TODOS os 7 CSVs esperados estão presentes
    no PVC de origem.
    """
    missing = [path for path in TABLE_SOURCE_MAP.values() if not os.path.exists(path)]

    if missing:
        logger.info("Aguardando arquivos ainda não disponíveis: %s", missing)
        return False

    logger.info("Todos os %d arquivos de origem estão disponíveis.", len(TABLE_SOURCE_MAP))
    return True
