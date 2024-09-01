import citybrain_platform
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from convert_cesm1_feature_zarr_to_parquet import convert_cesm1_feature_zarr_to_parquet_workflow
from upload_parquet_to_citybrain import upload_parquet_to_citybrain_workflow
from cesm1_varinfo_to_json import cesm1_varinfo_to_json_workflow
from cesm1_createtable_in_citybrain import cesm1_createtable_in_citybrain_workflow 
from cesm1_citybraintable_qa import cesm1_citybraintable_qa_workflow
import xarray as xr
import pandas as pd
import json
import time
import sys
import os
import gc

# Define default_args and DAG
default_args = {
    'owner': 'citybrain_cesm_team',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1), # Set to yesterday's date and time due to timezone difference
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
} 

dag = DAG(
    'cesm1_dag',
    default_args=default_args,
    description='cesm1_complete_dag',
    schedule_interval=None,  # Set to None for one-time execution
)

task_execute_bash_script = BashOperator(
    task_id='execute_bash_script',
    bash_command = "bash /root/airflow/dags/download_cesm1_data.sh 's3://ncar-cesm-lens/atm/daily/cesmLE-RCP85-QBOT.zarr'",
    dag=dag,
)

def extract_variables_from_json(**kwargs):
    with open('/root/airflow/dags/cesm1_variables.json', 'r') as f:
        variables = json.load(f)
    kwargs['ti'].xcom_push(key='variables', value=variables)
    
run_extract_variable_from_json = PythonOperator(
    task_id='extract_variables_from_json',
    python_callable=extract_variables_from_json,
    provide_context=True,
    dag=dag,
)

def find_table_info_in_csv(**kwargs):
    variables = kwargs['ti'].xcom_pull(task_ids='extract_variables_from_json', key='variables')
    s3path = variables.get('s3path')
    cesm1_varinfo_to_json_workflow(s3path)
    
run_find_table_info_in_csv = PythonOperator(
    task_id='find_table_info_in_csv',
    python_callable=find_table_info_in_csv,
    provide_context=True,
    dag=dag,
) 

def convert_cesm1_feature_zarr_to_parquet(**kwargs):
    
    variables = kwargs['ti'].xcom_pull(task_ids='extract_variables_from_json', key='variables')

    vartype = variables.get('vartype')
    component = variables.get('component')
    frequency = variables.get('frequency')
    tablename = variables.get('tablename')
    featurename = variables.get('featurename')

    # Call the convert function from py with the extracted variables
    convert_cesm1_feature_zarr_to_parquet_workflow(component,frequency,tablename,featurename)
        
run_convert_cesm1_feature_zarr_to_parquet = PythonOperator(
    task_id='convert_cesm1_feature_zarr_to_parquet',
    python_callable=convert_cesm1_feature_zarr_to_parquet,
    provide_context=True,
    dag=dag,
) 

def upload_parquet_to_citybrain(**kwargs):
    variables = kwargs['ti'].xcom_pull(task_ids='extract_variables_from_json', key='variables')

    vartype = variables.get('vartype')
    component = variables.get('component')
    frequency = variables.get('frequency')
    tablename = variables.get('tablename')
    featurename = variables.get('featurename')
    upload_parquet_to_citybrain_workflow(component,frequency,tablename,featurename)
        
run_upload_parquet_to_citybrain = PythonOperator(
    task_id='upload_parquet_to_citybrain',
    python_callable=upload_parquet_to_citybrain,
    provide_context=True,
    dag=dag,
)     

def cesm1_createtable_in_citybrain(**kwargs):
    cesm1_createtable_in_citybrain_workflow()
        
run_createtable_in_citybrain = PythonOperator(
    task_id='cesm1_createtable_in_citybrain',
    python_callable=cesm1_createtable_in_citybrain,
    provide_context=True,
    dag=dag,
) 

def cesm1_citybraintable_qa(**kwargs):
    cesm1_citybraintable_qa_workflow()
        
run_cesm1_citybraintable_qa = PythonOperator(
    task_id='cesm1_citybraintable_qa',
    python_callable=cesm1_citybraintable_qa,
    provide_context=True,
    dag=dag,
) 

# Set task dependencies

task_execute_bash_script >> run_extract_variable_from_json >> run_find_table_info_in_csv >> run_convert_cesm1_feature_zarr_to_parquet >> run_upload_parquet_to_citybrain >> run_createtable_in_citybrain >> run_cesm1_citybraintable_qa
