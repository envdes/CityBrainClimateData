import citybrain_platform
import xarray as xr
import pandas as pd
import numpy as np
import gc
import time
import os

from airflow.models import Variable

citybrain_platform.api_key = " "
citybrain_platform.api_baseurl = " "

def convert_cesm2_lnd_feature_zarr_to_parquet_workflow(forcing_variant,component,frequency,tablename,featurename):
    print(f"Received forcing_variant in convert_cesm2_feature_zarr_to_parquet_1D_to_3D.py: {forcing_variant}")
    print(f"Received component in convert_cesm2_feature_zarr_to_parquet_1D_to_3D.py: {component}")
    print(f"Received frequency in convert_cesm2_feature_zarr_to_parquet_1D_to_3D.py: {frequency}")
    print(f"Received tablename in convert_cesm2_feature_zarr_to_parquet_1D_to_3D.py: {tablename}")
    print(f"Received featurename in convert_cesm2_feature_zarr_to_parquet_1D_to_3D.py: {featurename}")

    newfolder = tablename+"_parquet"
    parent_dir = f"../../../datadisk/cesm2_parquet/{forcing_variant}/{component}/{frequency}"
    path = os.path.join(parent_dir, newfolder) 
    try:
        # Create a new folder
        os.makedirs(path, exist_ok=True)
        print(f"{parent_dir}/{newfolder} created") 
    except OSError as e:
        print(f"Error creating folder: {e}")

    class load_clm_subgrid_info:
        """This class is used for 1D to 3D conversion, for more information:
        https://zhonghuazheng.com/UrbanClimateExplorer/notebooks/CESM2_subgrid_info.html 
        """
        def __init__(self, ds, subgrid_info):
            self.ds = ds
            self.time = self.ds.time
            self.member_id = self.ds.member_id
            self.subgrid_info = subgrid_info
            self.lat = self.subgrid_info.lat
            self.lon = self.subgrid_info.lon
            self.ixy = self.subgrid_info.land1d_ixy
            self.jxy = self.subgrid_info.land1d_jxy
            self.ltype = self.subgrid_info.land1d_ityplunit
            self.ltype_dict = {value:key for key, value in self.ds.attrs.items() if 'ltype_' in key.lower()}     
        def get3d_var(self, clm_var_mask):
            var = self.ds[clm_var_mask]
            nmember = len(self.member_id.values)
            nlat = len(self.lat.values)
            nlon = len(self.lon.values)
            ntim = len(self.time.values)
            nltype = len(self.ltype_dict)
            # create an empty array
            gridded = np.full([nmember,ntim,nltype,nlat,nlon],np.nan)
            # assign the values
            gridded[:,
                    :,
                    self.ltype.values.astype(int) - 1, # Fortran arrays start at 1
                    self.jxy.values.astype(int) - 1,
                    self.ixy.values.astype(int) - 1] = var.values
            grid_dims = xr.DataArray(gridded, dims=("member_id","time","ltype","lat","lon"))
            grid_dims = grid_dims.assign_coords(member_id=self.member_id,
                                                time=self.time,
                                                ltype=[i for i in range(self.ltype.values.min(), 
                                                                        self.ltype.values.max()+1)],
                                                lat=self.lat.values,
                                                lon=self.lon.values)
            grid_dims.name = clm_var_mask
            return grid_dims.to_dataset()


    def workflow(subgrid_info,fullds,member_id_idx,slice_start,slice_end):
        # one slice 
        oneD_subds = fullds.isel(member_id=member_id_idx,time=slice(slice_start,slice_end))
        
        # 1D->3D
        clm_ls = [featurename]
        clm_var = clm_ls[0]
        testds = load_clm_subgrid_info(oneD_subds, subgrid_info)
        threeD_subds = testds.get3d_var(clm_var)
        
        # change the datetime coordinates and convert to dataframe
        threeD_subdf = threeD_subds[featurename].assign_coords(time = threeD_subds[featurename].indexes["time"].to_datetimeindex()).to_dataframe()
        
        del oneD_subds
        gc.collect()
        
        # save as parquet 
        newfolder = tablename+'_parquet'
        member_id = threeD_subdf.index[0][0]
        parquet_dir = f"../../../datadisk/cesm2_parquet/{forcing_variant}/{component}/{frequency}/{newfolder}"
        threeD_subdf.to_parquet(f"{parquet_dir}/member_id_{member_id}_time_{str(slice_start)}_{str(slice_end-1)}.parquet.gzip",
                    compression='gzip') 
        
        del threeD_subdf
        gc.collect()
    
    current_path = os.getcwd()
    print("Current working directory:", current_path)
    # current_path:'/datadisk/cesm2_raw/cmip6/atm/daily'
    # CESM2_subgrid_info.nc (https://zhonghuazheng.com/UrbanClimateExplorer/notebooks/CESM2_subgrid_info.html)
    subgrid_info = xr.open_dataset("../../../../CESM2_subgrid_info.nc")
    # load zarr
    rawdata_dir = f"../../../../cesm2_raw/{forcing_variant}/{component}/{frequency}/{tablename}.zarr"
    fullds = xr.open_dataset(rawdata_dir)
    batch_size = 215
    total_timestamps = len(fullds.isel(member_id=[0]).time)

    for member_id_idx in np.arange(len(fullds.member_id.values)):
        member_id = fullds.member_id.values[member_id_idx]
        for i in range(0, total_timestamps, batch_size):
            slice_start = i
            slice_end = i + batch_size
            if slice_end <= total_timestamps:
                starttime = time.time()
                workflow(subgrid_info,fullds,[member_id_idx],slice_start,slice_end)
                endtime = time.time()
                totaltime = (endtime - starttime)/60
                print(f"completed member_id_idx {member_id_idx} member_id {member_id} index {slice_start} - {slice_end-1} with {totaltime} minutes")
            else:
                slice_end = total_timestamps
                starttime = time.time()
                workflow(subgrid_info,fullds,member_id_idx,slice_start,slice_end)
                endtime = time.time()
                totaltime = (endtime - starttime)/60
                print(f"completed member_id_idx {member_id_idx} member_id {member_id} index {slice_start} - {slice_end-1} with {totaltime} minutes")