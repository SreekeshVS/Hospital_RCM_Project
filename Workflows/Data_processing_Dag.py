import airflow
from airflow import DAG
from airflow.utils import dates
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

#airflow Configurations
PROJECT_ID = "project-b2d1202e-c674-4674-a8a"
REGION = "us-central1"
CLUSTER_NAME = "my-hospital-dp-cluster"
COMPOSER_BUCKET = "us-central1-hospital-rcm-co-cc2246f3-bucket"
LANDING_BRONZE_SRC = f"gs://{COMPOSER_BUCKET}/data/DataProcessing/Landing_to_Bronze.sql"
BRONZE_SILVER_SRC = f"gs://{COMPOSER_BUCKET}/data/DataProcessing/Bronze_to_Silver.sql"
SILVER_GOLD_SRC = f"gs://{COMPOSER_BUCKET}/data/DataProcessing/Silver_to_Gold.sql"

#Read the SQL file from SRC
def read_sql_file(sql_file):
    with open(sql_file, "r") as f:
        return f.read()

BRONZE_SQL = read_sql_file(LANDING_BRONZE_SRC)
SILVER_SQL = read_sql_file(BRONZE_SILVER_SRC)
GOLD_SQL = read_sql_file(SILVER_GOLD_SRC)

#Default arguments
ARGS = {
    "owner": "SREEKESH",
    "start_date": None,
    "depends_on_past": False,
    "email": ["***@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="DataProcessing_Dag",
    schedule_interval=None,
    description="Data Processing DAG to process data present in Landing zone and then move to successive layers",
    default_args=ARGS,
    tags=["DataProcessing","etl","BigQuery","CDM"]
) as dag:


    bronze_task = BigQueryInsertJobOperator(
        task_id="Bronze",
        configuration={
            "query": {
                "query": BRONZE_SQL,
                "use_legacy_sql": False,
                "priority": "BATCH"
            }
        }
    )

    silver_task = BigQueryInsertJobOperator(
        task_id="Silver",
        configuration={
            "query": {
                "query": SILVER_SQL,
                "use_legacy_sql": False,
                "priority": "BATCH"
            }
        }
    )

    gold_task = BigQueryInsertJobOperator(
        task_id="Gold",
        configuration={
            "query": {
                "query": GOLD_SQL,
                "use_legacy_sql": False,
                "priority": "BATCH"
            }
        }
    )

#Defining the dependencies
bronze_task >> silver_task >> gold_task

