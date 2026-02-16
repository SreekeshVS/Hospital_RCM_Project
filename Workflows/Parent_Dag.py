import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils import dates
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Define the arguments
ARGS = {
    "owner": "SREEKESH",
    "start_date": datetime(2026, 2, 13),
    "depends_on_past": False,
    "email": ["***@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

#Define the DAG
with DAG(
    dag_id="parent_dag",
    schedule="0 5 * * *",
    description="Parent DAG, to orchestrate other dags",
    default_args=ARGS,
    tags=["parent", "orchestration"]
) as dag:

    #Define tasks:
    trigger_ingestion_dag = TriggerDagRunOperator(
        task_id="trigger_ingestion_dag",
        trigger_dag_id="Ingestion_Dag",
        wait_for_completion=True
    )

    trigger_dataprocessing_dag = TriggerDagRunOperator(
        task_id="trigger_dataprocessing_dag",
        trigger_dag_id="DataProcessing_Dag",
        wait_for_completion=True
    )

# Define dependencies
trigger_ingestion_dag >> trigger_dataprocessing_dag
