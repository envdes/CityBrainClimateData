import citybrain_platform
import xarray as xr
import pandas as pd
import gc
import time
import os

from airflow.models import Variable

citybrain_platform.api_key = " "
citybrain_platform.api_baseurl = " "

def convert_cesm1_feature_zarr_to_parquet_workflow(component,frequency,tablename,featurename):
    
    print(f"Received component in convert_cesm1_feature_zarr_to_parquet.py: {component}")
    print(f"Received frequency in convert_cesm1_feature_zarr_to_parquet.py: {frequency}")
    print(f"Received tablename in convert_cesm1_feature_zarr_to_parquet.py: {tablename}")
    print(f"Received featurename in convert_cesm1_feature_zarr_to_parquet.py: {featurename}")
    
    newfolder = tablename+"_parquet"
    parent_dir = f"../../../datadisk/cesm1_parquet/{component}/{frequency}"
    path = os.path.join(parent_dir, newfolder) 
    try:
        # create a new folder
        os.makedirs(path, exist_ok=True)
        print(f"{parent_dir}/{newfolder} created") 
    except OSError as e:
        print(f"Error creating folder: {e}")

    def workflow(component,frequency,tablename,featurename,slice_start, slice_end):
        # load zarr
        ds = xr.open_zarr(f'../../../datadisk/cesm1_raw/{component}/{frequency}/{tablename}.zarr').isel(time=slice(slice_start,slice_end))

        # change the datetime coordinates
        ds = ds.assign_coords(time = ds.indexes["time"].to_datetimeindex())

        # convert to dataframe
        df = ds[featurename].to_dataframe()
        del ds
        gc.collect()

        # save as parquet
        newfolder = tablename+'_parquet'
        df.to_parquet(f"../../../datadisk/cesm1_parquet/{component}/{frequency}/{newfolder}/{str(slice_start)}_{str(slice_end-1)}.parquet.gzip",
                    compression='gzip') 

        del df
        gc.collect()
        
    batch_size = 5
    total_timestamps = 34675

    for i in range(0, total_timestamps, batch_size):
        slice_start = i
        slice_end = i + batch_size
        if slice_end <= total_timestamps:
            starttime = time.time()
            workflow(component,frequency,tablename,featurename,slice_start, slice_end)
            endtime = time.time()
            totaltime = (endtime - starttime)/60
            print(f"completed index {slice_start} - {slice_end-1}; {totaltime} minutes")
        else:
            slice_end = total_timestamps
            starttime = time.time()
            workflow(component,frequency,tablename,featurename,slice_start, slice_end)
            endtime = time.time()
            totaltime = (endtime - starttime)/60
            print(f"completed index {slice_start} - {slice_end-1}; {totaltime} minutes")
