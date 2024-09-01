import citybrain_platform
from citybrain_platform.computing.data_types import Column, ColumnType,ExternalFiletype
import cftime
import xarray as xr
import pandas as pd
import gc
import glob
import time
import os
import json

citybrain_platform.api_key = " "
citybrain_platform.api_baseurl = " "

def cesm2_citybraintable_qa_workflow():
    with open('/root/airflow/dags/cesm2_variables.json', 'r') as f:
        variables = json.load(f)
        print(variables)
    
    component = variables['component']
    frequency = variables['frequency']
    tablename = variables['tablename']
    cirybrain_tablename = f"{component}_{frequency}_{tablename}"

    # create a folder to save computing results
    parent_dir = "/root/"
    newfolder = f"cesm2_QA/{cirybrain_tablename}"
    path = os.path.join(parent_dir, newfolder) 

    os.mkdir(path) 
    print(f"{parent_dir}{newfolder} created") 

    # check sample data
    job_id = citybrain_platform.Computing.create_job(
    sql=f"select * from {cirybrain_tablename} limit 5;" )
    print('check sample data job_id',job_id)

    while True:
        status = citybrain_platform.Computing.get_job_status(job_id=job_id)
        if status.status == "terminated":
            print(status.status)
            citybrain_platform.Computing.get_job_results(job_id=job_id, filepath=f"{path}/sampleresults.csv" )
            break
        time.sleep(10)

    # check columns
    cols = ["member_id","ltype","time","lat","lon"]
    for col in cols:
        print(col)
        job_id = citybrain_platform.Computing.create_job(
            sql=f"SELECT {col} from {cirybrain_tablename} GROUP BY {col};" )
        print(job_id)
        
        while True:
            status = citybrain_platform.Computing.get_job_status(job_id=job_id)
            if status.status == "terminated":
                print(status.status)
                citybrain_platform.Computing.get_job_results(job_id=job_id, filepath=f"{path}/{col}_results.csv" )
                break
            time.sleep(10)


