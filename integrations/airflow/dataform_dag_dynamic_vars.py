import datetime

from google.cloud.dataform_v1beta1 import WorkflowInvocation

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow import Dataset

import airflow
from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.decorators import task
from datetime import timedelta

DAG_ID = "data-transformation-pipeline"
PROJECT_ID = "whejna-modelling-sandbox"
REPOSITORY_ID = "training-repo"
DATAFORM_DATASET = "dataform_training"
REGION = "europe-west3"
GIT_COMMITISH = "full-version"
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

def get_config_params(**kwargs):   
    logical_date = kwargs["logical_date"]
    P_DESCRIPTION_PARAM = kwargs['dag_run'].conf.get('description_param', "Default Description")
    P_LOGICAL_DATE = logical_date.strftime("%d/%m/%Y")
    P_COMPILATION_RESULT = '{ "git_commitish": "' + GIT_COMMITISH + '" , "code_compilation_config": { "vars": { "logicalDate": "' + P_LOGICAL_DATE + '", "jobDescription": "' + P_DESCRIPTION_PARAM + '" } } }'
    
    context = get_current_context() 
    task_instance = context['task_instance']
    task_instance.xcom_push(key="compilation_result", value=P_COMPILATION_RESULT)

def print_params(task_instance):
     compilation_result = task_instance.xcom_pull('parse_input_params',key='compilation_result')
     print(f"compilation_result = {compilation_result}")

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
    schedule=[Dataset(f"dataform-training-data-ingestion")],
    default_args=default_args,
    catchup=False,
    tags=['dataform'],
) as dag:

    parse_params_op = PythonOperator( task_id = 'parse_input_params', python_callable = get_config_params )

    start = DummyOperator(
        task_id='start',
        dag=dag,
    )
     
    @task(task_id="create_compilation_result")
    def create_compilation_result(ti=None):
        p_compilation_result = eval(ti.xcom_pull('parse_input_params',key='compilation_result'))
    
        return DataformCreateCompilationResultOperator(
            task_id="t_create_compilation_result",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            compilation_result= p_compilation_result,
            gcp_conn_id="modelling_cloud_default",
        ).execute(ti)
        
    op_create_compilation_result = create_compilation_result()

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
         workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": {
                # "included_tags": ["reports"],
                "fully_refresh_incremental_tables_enabled": True
            },
        },
        asynchronous=True,
        gcp_conn_id="modelling_cloud_default",
    )
    
    is_workflow_invocation_done = DataformWorkflowInvocationStateSensor(
        task_id="is_workflow_invocation_done",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id=("{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}"),
        expected_statuses={WorkflowInvocation.State.SUCCEEDED, WorkflowInvocation.State.FAILED},
        gcp_conn_id="modelling_cloud_default",
    )
    
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )
     

start >> parse_params_op >> op_create_compilation_result >> create_workflow_invocation >> is_workflow_invocation_done >> end
