import citybrain_platform
from airflow.models import Variable
import glob
import time

citybrain_platform.api_key = " "
citybrain_platform.api_baseurl = " "
    
def upload_parquet_to_citybrain_workflow(component,frequency,tablename,featurename):

    print(f"Received component in convert_cesm1_feature_zarr_to_parquet.py: {component}")
    print(f"Received frequency in convert_cesm1_feature_zarr_to_parquet.py: {frequency}")
    print(f"Received tablename in convert_cesm1_feature_zarr_to_parquet.py: {tablename}")
    print(f"Received featurename in convert_cesm1_feature_zarr_to_parquet.py: {featurename}")

    remote_store_dir = f'test/{component}/{frequency}/{tablename}_parquet' 
    print(f'remote_store_dir = test/{component}/{frequency}/{tablename}_parquet')

    featurename_parquet = list(glob.glob(f"/datadisk/cesm1_parquet/{component}/{frequency}/{tablename}_parquet/*"))
    for local_file in featurename_parquet:
        filename = local_file.split("/", 6)[6]
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
    