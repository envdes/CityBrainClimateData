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

def cesm2_createtable_in_citybrain_workflow():

  # read data from variables.json
  with open('/root/airflow/dags/cesm2_variables.json', 'r') as f:
    variables = json.load(f)
    print(variables)
  
  featurename = variables['featurename']
  feature_explain = variables['feature_explain']
  component = variables['component']
  experiment = variables['experiment']
  frequency = variables['frequency']
  tablename = variables['tablename']
  forcing_variant = variables['forcing_variant']
  vertical_levels = variables['vertical_levels']
  spatial_domain = variables['spatial_domain']
  units = variables['units']
  start_time = variables['start_time']
  end_time = variables['end_time']
  cirybrain_tablename = f"{component}_{frequency}_{tablename}"
  remote_store_dir = f'test/{forcing_variant}/{component}/{frequency}/{tablename}_parquet' # 目录

  # Create table in citybrain
  columns = [
      Column("member_id", ColumnType.STRING, "this is an index"),
      Column("time", ColumnType.TIMESTAMP, "this is an index"),
      Column("ltype", ColumnType.BIGINT, "land type"),
      Column("lat", ColumnType.DOUBLE, "this is an index"),
      Column("lon", ColumnType.DOUBLE, "this is an index"),
      Column(f"{featurename}", ColumnType.DOUBLE, f"{feature_explain}")
  ]

  ok = citybrain_platform.Computing.create_table(
      name=cirybrain_tablename,
      columns=columns,
      description=f"{feature_explain}; component:{component}; experiment:{experiment}; forcing_variant:{forcing_variant},frequency:{frequency}; vertical_levels:{vertical_levels}; spatial_domain:{spatial_domain}; units:{units}; start_time:{start_time}; end_time: {end_time} ", # 表注释
      storage_filesource=remote_store_dir,
      storage_filetype=ExternalFiletype.PARQUET
  )
  print(ok)




