"""
DAG: banvic_ingestion

Orquestra a ingestão diária das 7 tabelas do ERP do BanVic (banvic_data.zip)
para o Data Warehouse simulado em Postgres via Meltano.

Fluxo:
    check_source_files (Sensor)
        -> run_meltano_elt (KubernetesPodOperator)
            -> validate_row_counts (PythonOperator)

Estratégia de idempotência:
    O loader (target-postgres) está configurado com load_method=upsert,
    fazendo merge pela chave primária de cada tabela.
    Reexecutar esta DAG para o mesmo dia NÃO duplica registros.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s

from utils.config import SOURCE_DATA_DIR
from utils.checks import all_source_files_exist
from utils.validations import validate_row_counts
from utils.callbacks import notify_failure

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}


def _source_data_pod_override(mount_path: str = SOURCE_DATA_DIR) -> dict:
    """
    Monta o PVC `banvic-source-data-pvc` (mesmos CSVs extraídos do
    banvic_data.zip) num pod comum do KubernetesExecutor.
    """
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        volume_mounts=[
                            k8s.V1VolumeMount(
                                name="banvic-source-data",
                                mount_path=mount_path,
                                read_only=True,
                            )
                        ],
                    )
                ],
                volumes=[
                    k8s.V1Volume(
                        name="banvic-source-data",
                        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                            claim_name="banvic-source-data-pvc"
                        ),
                    )
                ],
            )
        )
    }


with DAG(
    dag_id="banvic_ingestion",
    description="Ingestão diária das tabelas do ERP BanVic para o DW (Postgres) via Meltano",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["banvic", "ingestion", "meltano"],
    doc_md=__doc__,
) as dag:

    check_source_files = PythonSensor(
        task_id="check_source_files",
        python_callable=all_source_files_exist,
        poke_interval=30,
        timeout=60 * 10,
        mode="reschedule",
        executor_config=_source_data_pod_override(),
        doc_md="""
        Verifica se TODOS os 7 arquivos de origem (extraídos do
        banvic_data.zip) estão disponíveis antes de iniciar a extração.
        """,
    )

    run_meltano_elt = KubernetesPodOperator(
        task_id="run_meltano_elt",
        name="banvic-meltano-elt",
        namespace="banvic",
        image="banvic-meltano:latest",
        image_pull_policy="IfNotPresent",
        cmds=["meltano"],
        arguments=["--environment=k8s", "run", "banvic_full_ingestion"],
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name="postgres-credentials")
            )
        ],
        volume_mounts=[
            k8s.V1VolumeMount(name="banvic-data", mount_path="/project/data", read_only=True),
        ],
        volumes=[
            k8s.V1Volume(
                name="banvic-data",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="banvic-source-data-pvc"
                ),
            ),
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        doc_md="""
        Executa o job Meltano `banvic_full_ingestion` em um pod isolado.
        Reexecuções são seguras: o target usa `load_method: upsert`
        (merge por chave primária), então não há duplicação de dados.
        """,
    )

    validate_row_counts_task = PythonOperator(
        task_id="validate_row_counts",
        python_callable=validate_row_counts,
        executor_config=_source_data_pod_override(),
        doc_md="""
        Compara a contagem de linhas na origem (CSV) com o destino (Postgres)
        para cada uma das 7 tabelas, falhando a DAG caso alguma diferença
        não esperada seja encontrada.
        """,
    )

    check_source_files >> run_meltano_elt >> validate_row_counts_task
