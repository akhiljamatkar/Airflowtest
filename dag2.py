import airflow
from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)

PROJECT_ID = "aj23-458002"
REGION = "us-east1"
CLUSTER_NAME="Demo_cluster"
GCS_JOB_FILE='gs://dag_example2/dummy_pyspark_job_1.py'
GCS_JOB_FILE2='gs://dag_example2/dummy_pyspark_job_2.py'

Args={
    'owner':'Akhil Jamatkar',
    "email_on_failure": True,
    "email_on_retry": True,
    "email": "akhiljamatkar01@gmail.com",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "start_date": days_ago(1),
}
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "e2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
}
PYSPARK_JOB1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE},
}
PYSPARK_JOB2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE2},
}

with DAG(
    dag_id='create_cluseter',
    schedule_interval="0 5 * * *",
    description='start dag',
    default_args=Args
)as dag:
    
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id="pyspark_task_1", 
        job=PYSPARK_JOB1, 
        region=REGION, 
        project_id=PROJECT_ID
    )
    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id="pyspark_task_2", 
        job=PYSPARK_JOB2, 
        region=REGION, 
        project_id=PROJECT_ID
    )
    
    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

    create_cluster>>start_cluster>>pyspark_task_1>>pyspark_task_2>>stop_cluster>>delete_cluster