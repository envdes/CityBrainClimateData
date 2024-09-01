import citybrain_platform
import glob
import time

from airflow.models import Variable

citybrain_platform.api_key = " "
citybrain_platform.api_baseurl = " "
    
def upload_cesm2_lnd_parquet_to_citybrain_workflow(forcing_variant,component,frequency,tablename,featurename):
   
    print(f"Received component in convert_cesm2_lnd_feature_zarr_to_parquet.py: {forcing_variant}")
    print(f"Received component in convert_cesm2_lnd_feature_zarr_to_parquet.py: {component}")
    print(f"Received frequency in convert_cesm2_lnd_feature_zarr_to_parquet.py: {frequency}")
    print(f"Received tablename in convert_cesm2_lnd_feature_zarr_to_parquet.py: {tablename}")
    print(f"Received featurename in convert_cesm2_lnd_feature_zarr_to_parquet.py: {featurename}")

    remote_store_dir = f'test/{forcing_variant}/{component}/{frequency}/{tablename}_parquet'
    print(f'remote_store_dir = test/{forcing_variant}/{component}/{frequency}/{tablename}_parquet')

    featurename_parquet = list(glob.glob(f"/datadisk/cesm2_parquet/{forcing_variant}/{component}/{frequency}/{tablename}_parquet/*"))
    for local_file in featurename_parquet:
        filename = local_file.split("/", 7)[7]
        print(f"{filename}")
        # Upload parquet files
        remote_path=f"{remote_store_dir}/{filename}"
        starttime = time.time()
        res = citybrain_platform.Storage.upload_file(
            remote_path=remote_path, 
            local_file=local_file)
        endtime = time.time()
        totaltime = (endtime - starttime)/60
        print(f"{res},{remote_path} uploaded; {totaltime} minutes")
    