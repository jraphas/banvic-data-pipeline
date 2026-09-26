"""
Callbacks de monitoramento.
"""

import logging

logger = logging.getLogger(__name__)

def notify_failure(context) -> None:
    """
    Registrado como `on_failure_callback` nos default_args da DAG.
    Roda em qualquer falha de task com contexto completo da execução.
    """
    task_instance = context["task_instance"]
    dag_run = context["dag_run"]

    logger.error(
        "FALHA na DAG '%s' | task='%s' | logical_date=%s | try=%s/%s | log_url=%s",
        dag_run.dag_id,
        task_instance.task_id,
        context["logical_date"],
        task_instance.try_number,
        task_instance.max_tries + 1,
        task_instance.log_url,
    )

