import json
import pandas as pd
import geopandas as gpd
import numpy as np
import xarray as xr
from datetime import datetime,timedelta
import os
try:
    import climateserv2.locallog.locallogging as llog
    import climateserv2.parameters as params
except:
    import locallog.locallogging as llog
    import parameters as params

logger=llog.getNamedLogger("request_processor")

def get_filelist(dataset,datatype,start_date,end_date):
    year_nums = range( datetime.strptime(start_date, '%Y-%m-%d').year,  datetime.strptime(end_date, '%Y-%m-%d').year+1)
    filelist=[]
    dsname = dataset.split('_')
    if "ucsb-chirps"==dsname[0]:
        for year in year_nums:
            name = params.dataTypes[datatype]['inputDataLocation'] + "ucsb_chirps" + ".global." + dsname[
                2] + ".daily."+str(year)+".nc4"
            if os.path.exists(name):
                filelist.append(name)
    elif "ucsb-chirp"==dsname[0]:
        for year in year_nums:
            for month in range(12):
                name = params.dataTypes[datatype]['inputDataLocation'] + "ucsb_chirp" + ".global." + dsname[
                    2] + ".daily."+str(year)+str('{:02d}'.format(month+1))+".nc4"
                if os.path.exists(name):
                    filelist.append(name)
    elif "ucsb-chirps-gefs"==dsname[0]:
        for year in year_nums:
            for month in range(12):
                name = params.dataTypes[datatype]['inputDataLocation'] + "ucsb-chirps-gefs" + ".global." + dsname[
                    2] + ".10dy."+str(year)+str('{:02d}'.format(month+1))+".nc4"
                if os.path.exists(name):
                    filelist.append(name)
    elif "usda-smap"==dsname[0]:
        for year in year_nums:
            name = params.dataTypes[datatype]['inputDataLocation'] + dsname[0] + ".global." + dsname[2] + ".3dy."+str(year)+".nc4"
            if os.path.exists(name):
                filelist.append(name)
    elif "nmme-ccsm4_bcsd"==dsname[0]:
        name = params.nmme_ccsm4_path + dataset
        if os.path.exists(name):
            filelist.append(name)
    elif "nmme-cfsv2_bcsd"==dsname[0]:
        name = params.nmme_cfsv2_path + dataset
        if os.path.exists(name):
            filelist.append(name)
    elif "imerg" in dataset:
        for year in year_nums:
            name = params.dataTypes[datatype]['inputDataLocation'] + dsname[0] + ".global." + dsname[2] + ".1dy."+str(year)+".nc4"
            if os.path.exists(name):
                filelist.append(name)
    else:
        days_list = [i.strftime("%Y%m%d") for i in pd.date_range(start=start_date, end=end_date, freq='D')]
        for day in days_list:
            if "ndvi" in dataset:
                name=params.dataTypes[datatype]['inputDataLocation'] + dsname[0]+"."+day+"T000000Z." + dsname[1] +".250m.10dy.nc4"
            elif "sport-esi" in dataset and "12wk" in dataset:
                name = params.dataTypes[datatype]['inputDataLocation'] + dsname[0] + "." + day + "T000000Z.global." +  dsname[2] + ".12wk.nc4"
            elif "sport-esi" in dataset and "4wk" in dataset:
                name = params.dataTypes[datatype]['inputDataLocation'] + dsname[0] + "." + day + "T000000Z.global." + dsname[2] + ".4wk.nc4"
            if os.path.exists(name):
                filelist.append(name)
    return filelist

# To get the dates and values corresponding to the dataset, variable, dates, operation and geometry
def get_thredds_values(start_date, end_date, variable, geom, operation,file_list):
    # Convert dates to %Y-%m-%d format for THREDDS URL
    try:
        st = datetime.strptime(start_date, '%m/%d/%Y')
        et = datetime.strptime(end_date, '%m/%d/%Y')
        start_date = datetime.strftime(st, '%Y-%m-%d')
        end_date = datetime.strftime(et, '%Y-%m-%d')
    except:
        # If there is an exception with date format while converting, we can just ignore the conversion and use the passed dates
        pass
    jsonn =json.loads(str(geom))
    for x in range(len(jsonn["features"])):
        if "properties" not in jsonn["features"][x]:
            jsonn["features"][x]["properties"] = {}

    # If the geometry is not a point, map the area of interest to the netCDF and extract values
    # If the geometry is a point, get dates and values from the openDAP URL as shown below (line 120)
    json_aoi = json.dumps(jsonn)
    geodf = gpd.read_file(json_aoi)
    lon1, lat1, lon2, lat2 = geodf.total_bounds
    # using xarray to open the temporary netcdf
    try:
        nc_file = xr.open_mfdataset(file_list)
    except Exception as e :
        print(str(e))
        return [],[]
    lat_bounds = nc_file.sel(latitude=[lat1, lat2], method='nearest').latitude.values
    lon_bounds = nc_file.sel(longitude=[lon1, lon2], method='nearest').longitude.values
    latSlice = slice(lat_bounds[0], lat_bounds[1])
    lonSlice = slice(lon_bounds[0], lon_bounds[1])
    data = nc_file[variable].sel(longitude=lonSlice,latitude=latSlice).sel(time=slice(start_date,end_date))
    dates = data.time.dt.strftime("%Y-%m-%d").values.tolist()
    if operation == "min":
        ds_vals = data.min(dim=['latitude', 'longitude']).values
        ds_vals[np.isnan(ds_vals)] = -9999
        return dates, ds_vals
    elif operation == "avg":
        ds_vals = data.mean(dim=['latitude', 'longitude']).values
        ds_vals[np.isnan(ds_vals)] = -9999
        return dates, ds_vals
    elif operation == "max":
        ds_vals = data.max(dim=['latitude', 'longitude']).values
        ds_vals[np.isnan(ds_vals)] = -9999
        return dates, ds_vals
    elif operation == "download":
        if jsonn['features'][0]['geometry']['type'] == "Point":
            ds_vals = data.values
            ds_vals[np.isnan(ds_vals)] = -9999
            return dates, ds_vals
        return data

# To retrive the CHIRPS data from 1981  to 2020 for Monthly Analysis.
# Retrieves 25th, 50th, 75th percentiles corresponding to month list from NMME
def get_chirps_climatology(month_nums,total_bounds):

    basepath='/mnt/climateserv/process_tmp/downloads/chirps/ucsb-chirps-monthly-resolved-for-climatology.nc4'
    ds = xr.open_dataset(basepath)
    lon1, lat1, lon2, lat2 = total_bounds
    lat_bounds = ds.sel(latitude=[lat1, lat2], method='nearest').latitude.values
    lon_bounds = ds.sel(longitude=[lon1, lon2], method='nearest').longitude.values
    latSlice = slice(lat_bounds[0], lat_bounds[1])
    lonSlice = slice(lon_bounds[0], lon_bounds[1])
    precip = ds.precipitation_amount.sel(longitude=lonSlice,latitude=latSlice).mean(dim=['latitude', 'longitude'])
    LTA=precip.groupby('time.month').mean(dim='time').values  # this give the mean climatology for the spatial averages
    LTA[np.isnan(LTA)] = -9999
    value = precip.chunk(dict(time=-1)).groupby('time.month').quantile(q=[0.25, 0.50, 0.75], dim='time').values
    value[np.isnan(value)] = -9999
    resarr = [list(value[month_nums[0] - 1]), list(value[month_nums[1] - 1]), list(value[month_nums[2] - 1]), list(value[month_nums[3] - 1]),
              list(value[month_nums[4] - 1]), list(value[month_nums[5] - 1])]
    LTAarr=[LTA[month_nums[0]-1],LTA[month_nums[1]-1],LTA[month_nums[2]-1],LTA[month_nums[3]-1],LTA[month_nums[4]-1],LTA[month_nums[5]-1]]
    return resarr,LTAarr

# To retrieve NMME data from start date of the netCDF to 180 days from start date
# Requires bounds of the geometry to get the data from CCSM4 and CFSV2 sensor files.
# Uses first five ensembles from both sensors
def get_nmme_data(total_bounds):
    # set number of ensembles to use from each dataset.
    numEns = 5
    LTA=[]
    for iens in np.arange(numEns):
        lon1, lat1, lon2, lat2 = total_bounds
    basepath = '/mnt/climateserv/process_tmp/fast_nmme_monthly/nmme-mme_bcsd.latest.global.0.5deg.daily.nc4'
    ds = xr.open_dataset(basepath)
    lat_bounds = ds.sel(latitude=[lat1, lat2], method='nearest').latitude.values
    lon_bounds = ds.sel(longitude=[lon1, lon2], method='nearest').longitude.values
    latSlice = slice(lat_bounds[0], lat_bounds[1])
    lonSlice = slice(lon_bounds[0], lon_bounds[1])
    try:
        nmme_values = ds.precipitation.sel(longitude=lonSlice,latitude=latSlice).mean(dim=['latitude', 'longitude']).values
    except Exception as e:
        print(e)
    nmme_values[np.isnan(nmme_values)] = -9999
    return (nmme_values).tolist(),LTA

def get_monthlyanalysis_dates_bounds(geom):

    # Get start date and end date for NMME from netCDf file
    nc_file = xr.open_dataset(params.nmme_ccsm4_path+'nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens001.nc4')
    start_date = nc_file["time"].values.min()
    t = pd.to_datetime(str(start_date))
    start_date = t.strftime('%Y-%m-%d')
    ed = datetime.strptime(start_date,'%Y-%m-%d') +  timedelta(days = 180)
    end_date = ed.strftime('%Y-%m-%d')

    month_list = [i.strftime("%Y-%m-%d") for i in pd.date_range(start=start_date, end=end_date, freq='MS')]
    month_nums =[int(i.strftime("%m")) for i in pd.date_range(start=start_date, end=end_date, freq='MS')]

    # add properties to geometry json if it did not exist
    jsonn = json.loads(str(geom))
    for x in range(len(jsonn["features"])):
        if "properties" not in jsonn["features"][x]:
            jsonn["features"][x]["properties"] = {}
    json_aoi = json.dumps(jsonn)
    # using geopandas to get the bounds
    aoi = gpd.read_file(json_aoi)
    return month_list,month_nums,aoi.total_bounds