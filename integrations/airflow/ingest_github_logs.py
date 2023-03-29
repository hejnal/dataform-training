import datetime

from airflow import models
from airflow.operators.dummy_operator import DummyOperator

import airflow
from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import timedelta
from airflow.operators import bash
from airflow.operators import python
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow import Dataset

DAG_ID = "ingest_github_logs"
PROJECT_ID = "whejna-modelling-sandbox"
REGION_ID = "europe-west3"
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'whejna',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=0),
    'start_date': YESTERDAY,
}

with models.DAG(
    DAG_ID,
    schedule_interval='@once',
    default_args=default_args,
    catchup=False,
    tags=['ingestion'],
) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )
    
    execute_cloud_run_jobs = bash.BashOperator(
        task_id='ingest_github_logs', 
        bash_command=f"gcloud beta run jobs execute gitlogs --project={PROJECT_ID} --region={REGION_ID} --wait" # The end point of the deployed Cloud Run container
    ) 
    
    end = DummyOperator(
        task_id='end',
        dag=dag,
        outlets=[Dataset("dataform-training-data-ingestion")]
    )
     

start >> execute_cloud_run_jobs >> end