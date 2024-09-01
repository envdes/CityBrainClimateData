import citybrain_platform
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from convert_cesm2_lnd_feature_zarr_to_parquet import convert_cesm2_lnd_feature_zarr_to_parquet_workflow
from upload_cesm2_lnd_parquet_to_citybrain import upload_cesm2_lnd_parquet_to_citybrain_workflow
from cesm2_varinfo_to_json import cesm2_varinfo_to_json_workflow
from cesm2_createtable_in_citybrain import cesm2_createtable_in_citybrain_workflow 
from cesm2_citybraintable_qa import cesm2_citybraintable_qa_workflow
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
    'start_date': datetime.now() - timedelta(days=1),  # Set to yesterday's date and time due to timezone difference
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
} 

dag = DAG(
    'cesm2_lnd_dag_lat_lng_included',
    default_args=default_args,
    description='cesm2_lnd_dag_lat_lng_included_in_zarr',
    schedule_interval=None,  # Set to None for one-time execution
)

download_cesm2_from_aws = BashOperator(
    task_id='execute_bash_script',
    bash_command = "bash /root/airflow/dags/download_cesm2_data.sh 's3://ncar-cesm2-lens/lnd/daily/cesm2LE-ssp370-cmip6-FSNO.zarr'",
    dag=dag,
)

def extract_variables_from_json(**kwargs):
    with open('/root/airflow/dags/cesm2_variables.json', 'r') as f:
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
    cesm2_varinfo_to_json_workflow(s3path)
    
run_find_table_info_in_csv = PythonOperator(
    task_id='find_table_info_in_csv',
    python_callable=find_table_info_in_csv,
    provide_context=True,
    dag=dag,
) 

def convert_cesm2_lnd_feature_zarr_to_parquet(**kwargs):
    
    variables = kwargs['ti'].xcom_pull(task_ids='extract_variables_from_json', key='variables')

    vartype = variables.get('vartype')
    component = variables.get('component')
    frequency = variables.get('frequency')
    tablename = variables.get('tablename')
    featurename = variables.get('featurename')
    experiment = variables.get('experiment')
    forcing_variant = variables.get('forcing_variant')
    featurename = variables.get('featurename')

    convert_cesm2_lnd_feature_zarr_to_parquet_workflow(forcing_variant,component,frequency,tablename,featurename)
        
run_convert_cesm2_feature_zarr_to_parquet = PythonOperator(
    task_id='convert_cesm2_lnd_feature_zarr_to_parquet',
    python_callable=convert_cesm2_lnd_feature_zarr_to_parquet,
    provide_context=True,
    dag=dag,
) 

def upload_cesm2_lnd_parquet_to_citybrain(**kwargs):
    variables = kwargs['ti'].xcom_pull(task_ids='extract_variables_from_json', key='variables')

    forcing_variant = variables.get('forcing_variant')
    vartype = variables.get('vartype')
    component = variables.get('component')
    frequency = variables.get('frequency')
    tablename = variables.get('tablename')
    featurename = variables.get('featurename')
    upload_cesm2_lnd_parquet_to_citybrain_workflow(forcing_variant,component,frequency,tablename,featurename)
        
run_upload_cesm2_lnd_parquet_to_citybrain = PythonOperator(
    task_id='upload_cesm2_lnd_parquet_to_citybrain',
    python_callable=upload_cesm2_lnd_parquet_to_citybrain,
    provide_context=True,
    dag=dag,
)     

def cesm2_createtable_in_citybrain(**kwargs):
    cesm2_createtable_in_citybrain_workflow()
        
run_createtable_in_citybrain = PythonOperator(
    task_id='cesm2_createtable_in_citybrain',
    python_callable=cesm2_createtable_in_citybrain,
    provide_context=True,
    dag=dag,
) 

def cesm2_citybraintable_qa(**kwargs):
    cesm2_citybraintable_qa_workflow()
        
run_cesm2_citybraintable_qa = PythonOperator(
    task_id='cesm2_citybraintable_qa',
    python_callable=cesm2_citybraintable_qa,
    provide_context=True,
    dag=dag,
) 

download_cesm2_from_aws >> run_extract_variable_from_json >> run_find_table_info_in_csv >> run_convert_cesm2_feature_zarr_to_parquet >> run_upload_cesm2_lnd_parquet_to_citybrain >> run_createtable_in_citybrain >> run_cesm2_citybraintable_qa