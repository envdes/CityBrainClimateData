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

def cesm1_createtable_in_citybrain_workflow():

  # example:
  # component = 'atm'
  # frequency = 'daily'
  # tablename = 'cesmLE_RCP85_PRECSC'
  # featurename = 'PRECSC'
  # experiment = 'RCP85'
  # feature_explain = "convective snow rate (water equivalent)"
  # vertical_levels = '1'
  # spatial_domain = 'global'
  # units = 'W/m2'
  # start_time = '1/1/06 12:00'
  # end_time = '12/31/00 12:00'
  
  # read data from variables.json
  with open('/root/airflow/dags/cesm1_variables.json', 'r') as f:
    variables = json.load(f)
    print(variables)
  
  component = variables['component']
  frequency = variables['frequency']
  tablename = variables['tablename']
  featurename = variables['featurename']
  experiment = variables['experiment']
  feature_explain = variables['feature_explain']
  vertical_levels = variables['vertical_levels']
  spatial_domain = variables['spatial_domain']
  units = variables['units']
  start_time = variables['start_time']
  end_time = variables['end_time']
  cirybrain_tablename = f"{component}_{frequency}_{tablename}"
  remote_store_dir = f'test/{component}/{frequency}/{tablename}_parquet' 

  # Create table in citybrain
  columns = [
      Column("member_id", ColumnType.BIGINT, "this is an index"),
      Column("time", ColumnType.TIMESTAMP, "this is an index"),
      Column("lat", ColumnType.DOUBLE, "this is an index"),
      Column("lon", ColumnType.DOUBLE, "this is an index"),
      Column(f"{featurename}", ColumnType.DOUBLE, f"{feature_explain}")
  ]

  ok = citybrain_platform.Computing.create_table(
      name=cirybrain_tablename, 
      columns=columns, 
      description=f"{feature_explain}; component:{component}; experiment:{experiment}; frequency:{frequency}; vertical_levels:{vertical_levels}; spatial_domain:{spatial_domain}; units:{units}; start_time:{start_time}; end_time: {end_time} ", # 表注释
      storage_filesource=remote_store_dir, 
      storage_filetype=ExternalFiletype.PARQUET
  )
  print(ok)

  # Make Table Public Or Private To Others
  # public_table_name = citybrain_platform.Computing.public_table(name=cirybrain_tablename)
  # public_table_name = citybrain_platform.Computing.update_table_status(name=cirybrain_tablename, public=True)
  # print(f"make table {cirybrain_tablename} public")



