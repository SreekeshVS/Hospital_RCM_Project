import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils import dates
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator
)

# DAG configurations
PROJECT_ID = "project-b2d1202e-c674-4674-a8a"
REGION = "us-central1"
CLUSTER_NAME = "my-hospital-dp-cluster"
COMPOSER_BUCKET = "us-central1-hospital-rcm-co-cc2246f3-bucket"
JOB_DBA_LANDING_SRC = f"gs://{COMPOSER_BUCKET}/data/Ingestion/HospitalAtoLanding.py"
JOB_DBB_LANDING_SRC = f"gs://{COMPOSER_BUCKET}/data/Ingestion/HospitalBtoLanding.py"
JOB_CLAIMS_LANDING_SRC = f"gs://{COMPOSER_BUCKET}/data/Ingestion/ClaimsToBronze.py"
JOB_CPT_LANDING_SRC = f"gs://{COMPOSER_BUCKET}/data/Ingestion/CptCodesToBronze.py"

JOB_DBA_LANDING = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": JOB_DBA_LANDING_SRC},
}

JOB_DBB_LANDING = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": JOB_DBB_LANDING_SRC},
}

JOB_CLAIMS_LANDING = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": JOB_CLAIMS_LANDING_SRC},
}

JOB_CPT_LANDING = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": JOB_CPT_LANDING_SRC},
}


ARGS = {
    "owner": "SREEKESH",
    "start_date": None,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "email": ["***@gmail.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

#defining the DAG
with DAG(
    dag_id="Ingestion_Dag",
    schedule=None,
    description="Hospital Ingestion DAG to extract data from MySQL DB to GCS Landing zone",
    default_args=ARGS,
    tags=["Hospital","pyspark","ingestion","dataproc"],
) as dag:

    #define the tasks
    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    ingestion_task_a = DataprocSubmitJobOperator(
        task_id="ingestion_task_a",
        job=JOB_DBA_LANDING,
        project_id=PROJECT_ID,
        region=REGION
    )

    ingestion_task_b = DataprocSubmitJobOperator(
        task_id="ingestion_task_b",
        job=JOB_DBB_LANDING,
        project_id=PROJECT_ID,
        region=REGION
    )

    ingestion_task_claims = DataprocSubmitJobOperator(
        task_id="ingestion_task_claims",
        job=JOB_CLAIMS_LANDING,
        project_id=PROJECT_ID,
        region=REGION
    )

    ingestion_task_cpt = DataprocSubmitJobOperator(
        task_id="ingestion_task_cpt",
        job=JOB_CPT_LANDING,
        project_id=PROJECT_ID,
        region=REGION
    )

    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

#define the task dependencies:

start_cluster >> ingestion_task_a >> ingestion_task_b >> ingestion_task_claims >> ingestion_task_cpt >> stop_cluster
