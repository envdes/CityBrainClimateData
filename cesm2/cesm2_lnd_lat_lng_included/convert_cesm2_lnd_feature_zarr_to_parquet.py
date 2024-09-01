import citybrain_platform
import xarray as xr
import pandas as pd
import numpy as np
import gc
import time
import os
import glob
from airflow.models import Variable

citybrain_platform.api_key = " "
citybrain_platform.api_baseurl = " "

def convert_cesm2_lnd_feature_zarr_to_parquet_workflow(forcing_variant,component,frequency,tablename,featurename):
    
    print(f"Received forcing_variant in convert_cesm2_feature_zarr_to_parquet.py: {forcing_variant}")
    print(f"Received component in convert_cesm2_feature_zarr_to_parquet.py: {component}")
    print(f"Received frequency in convert_cesm2_feature_zarr_to_parquet.py: {frequency}")
    print(f"Received tablename in convert_cesm2_feature_zarr_to_parquet.py: {tablename}")
    print(f"Received featurename in convert_cesm2_feature_zarr_to_parquet.py: {featurename}")
    
    newfolder = tablename+"_parquet"
    parent_dir = glob.glob(f"/datadisk/cesm2_parquet/{forcing_variant}/{component}/{frequency}")[0]
    path = os.path.join(parent_dir, newfolder) 
    try:
        # Create a new folder
        os.makedirs(path, exist_ok=True)
        print(f"{parent_dir}/{newfolder} created") 
    except OSError as e:
        print(f"Error creating folder: {e}")

    def workflow(forcing_variant,component,frequency,tablename,featurename,slice_start, slice_end):
        # load zarr
        zarr_path = glob.glob(f"/datadisk/cesm2_raw/{forcing_variant}/{component}/{frequency}/{tablename}.zarr")[0]
        ds = xr.open_zarr(zarr_path).isel(time=slice(slice_start,slice_end))
        # change the datetime coordinates 
        ds = ds.assign_coords(time = ds.indexes["time"].to_datetimeindex())
        # convert to dataframe 
        df = ds[featurename].to_dataframe()
        del ds

        gc.collect()
        # save as parquet 
        newfolder = tablename+'_parquet'
        parquet_path = f"../../../datadisk/cesm2_parquet/{forcing_variant}/{component}/{frequency}/{newfolder}/{str(slice_start)}_{str(slice_end-1)}.parquet.gzip"
        df.to_parquet(parquet_path,compression='gzip') 
        del df

        gc.collect()

    rawdata_dir = glob.glob(f"/datadisk/cesm2_raw/{forcing_variant}/{component}/{frequency}/{tablename}.zarr")[0]
    fullds = xr.open_dataset(rawdata_dir)
    batch_size = 43
    total_timestamps = len(fullds.isel(member_id=[0]).time)

    for i in range(0, total_timestamps, batch_size):
        slice_start = i
        slice_end = i + batch_size
        if slice_end <= total_timestamps:
            starttime = time.time()
            workflow(forcing_variant,component,frequency,tablename,featurename,slice_start, slice_end)
            endtime = time.time()
            totaltime = (endtime - starttime)/60
            print(f"completed index {slice_start} - {slice_end-1}; {totaltime} minutes")
        else:
            slice_end = total_timestamps
            starttime = time.time()
            workflow(forcing_variant,component,frequency,tablename,featurename,slice_start, slice_end)
            endtime = time.time()
            totaltime = (endtime - starttime)/60
            print(f"completed index {slice_start} - {slice_end-1}; {totaltime} minutes")