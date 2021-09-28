import json
import pandas as pd
import geopandas as gpd
import numpy as np
import os
import xarray as xr
from datetime import datetime,timedelta
from shapely.geometry import mapping

try:
    import climateserv2.locallog.locallogging as llog
    import climateserv2.parameters as params
except:
    import locallog.locallogging as llog
    import parameters as params

logger=llog.getNamedLogger("request_processor")

# To retrieve the THREDDS URL to download netcdf file corresponding to the dataset, variable, dates and geometry
def get_tds_request(start_date, end_date, dataset, variable, geom):
    tds_request=""
    jsonn = json.loads(str(geom))
    # add properties to geometry json if it did not exist
    for x in range(len(jsonn["features"])):
        if "properties" not in jsonn["features"][x]:
            jsonn["features"][x]["properties"] = {}
    # Convert dates to %Y-%m-%d format for THREDDS URL
    try:
        st = datetime.strptime(start_date, '%m/%d/%Y')
        et = datetime.strptime(end_date, '%m/%d/%Y')
        start_date = datetime.strftime(st, '%Y-%m-%d')
        end_date = datetime.strftime(et, '%Y-%m-%d')
    except:
        # If there is an exception with date format while converting, we can just ignore the conversion and use the passed dates
        pass

    # THREDDS URL takes latitude and longitude for a point and bounds for a polygon
    if jsonn['features'][0]['geometry']['type'] == "Point":
        coords = jsonn['features'][0]['geometry']['coordinates']
        lat, lon = coords[0], coords[1]
        tds_request = "http://thredds.servirglobal.net/thredds/ncss/Agg/" + dataset + "?var=" + variable + "&latitude=" + str(
            lat) + "&longitude=" + str(
            lon) + "&time_start=" + start_date + "T00%3A00%3A00Z&time_end=" + end_date + "T00%3A00%3A00Z&accept=netcdf"
    else:
        try:
            json_aoi = json.dumps(jsonn)
            # Using geopandas to get the bounds
            aoi = gpd.read_file(json_aoi)
            east = aoi.total_bounds[2]
            south = aoi.total_bounds[1]
            west = aoi.total_bounds[0]
            north = aoi.total_bounds[3]

            tds_request = "http://thredds.servirglobal.net/thredds/ncss/Agg/" + dataset + "?var=" + variable + "&north=" + str(
                north) + "&west=" + str(west) + "&east=" + str(east) + "&south=" + str(
                south) + "&disableProjSubset=on&horizStride=1&time_start=" + start_date + "T00%3A00%3A00Z&time_end=" + end_date + "T00%3A00%3A00Z&timeStride=1"
        except Exception as e:
            logger.info(e)
    return tds_request

# To get the dates and values corresponding to the dataset, variable, dates, operation and geometry
def get_aggregated_values(start_date, end_date, dataset, variable, geom, operation, temp_file):
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
    if jsonn['features'][0]['geometry']['type']!="Point":
        json_aoi = json.dumps(jsonn)
        geodf = gpd.read_file(json_aoi)
        lon1, lat1, lon2, lat2 = geodf.total_bounds
        if "nmme-ccsm4" in dataset:
            try:
                nc_file = xr.open_dataset(params.nmme_ccsm4_path + dataset)
            except Exception as e :
                print(str(e))
                return [],[]
            data = nc_file[variable].sel(time=slice(start_date,end_date)).sel(latitude=slice(lat1, lat2), longitude=slice(lon1, lon2))
            dates = data.time.dt.strftime("%Y-%m-%d").values.tolist()

            if operation == "min":
                return dates,data.min(dim=['latitude','longitude']).values
            elif operation == "avg":
                return dates,data.mean(dim=['latitude','longitude']).values
            elif operation == "max":
                return dates,data.max(dim=['latitude','longitude']).values
            else:
                return data
        elif "nmme-cfsv2" in dataset:
            try:
                nc_file = xr.open_dataset(params.nmme_cfsv2_path + dataset)
            except Exception as e :
                print(str(e))
                return [],[]
            data = nc_file[variable].sel(time=slice(start_date,end_date)).sel(latitude=slice(lat1, lat2), longitude=slice(lon1, lon2))
            dates = data.time.dt.strftime("%Y-%m-%d").values.tolist()

            if operation == "min":
                return dates,data.min(dim=['latitude','longitude']).values
            elif operation == "avg":
                return dates,data.mean(dim=['latitude','longitude']).values
            elif operation == "max":
                return dates,data.max(dim=['latitude','longitude']).values
            elif operation == "download":
                return data

        else:
            # using xarray to open the temporary netcdf
            try:
                days_list = [i.strftime("%Y%m%d") for i in pd.date_range(start=start_date, end=end_date, freq='D')]
                file_list=[]
                dsname=dataset.split('_')
                for day in days_list:
                    name=params.chirps_path + dsname[0]+"."+day+"T000000Z.global."+dsname[2]+".daily.nc4"
                    file_list.append(name)
                nc_file = xr.open_mfdataset(file_list)

            except Exception as e :
                print(str(e))
                return [],[]
            print(geodf.total_bounds)
            data = nc_file[variable].sel(time=slice(start_date,end_date)).sel(latitude=slice(lat1, lat2), longitude=slice(lon1, lon2))
            dates = data.time.dt.strftime("%Y-%m-%d").values.tolist()
            print(dates)
            print(operation)
            print(data)
            if operation == "min":
                return dates,data.min(dim=['latitude','longitude']).values
            elif operation == "avg":
                return dates,data.mean(dim=['latitude','longitude']).values
            elif operation == "max":
                print(data.max(dim=['latitude','longitude']))
                return dates,data.max(dim=['latitude','longitude']).values
            elif operation == "download":
                return data
    elif jsonn['features'][0]['geometry']['type']=="Point":
        dates = []
        try:
            coords = jsonn['features'][0]['geometry']['coordinates']
            lon, lat = coords[0], coords[1]
            if "nmme-ccsm4" in dataset:
                ds = xr.open_dataset(params.nmme_ccsm4_path + dataset)
                point = ds[variable].sel(time=slice(start_date, end_date)).sel(longitude=lon,latitude=lat,method='nearest')
            elif "nmme-cfsv2" in dataset:
                ds = xr.open_dataset(params.nmme_cfsv2_path + dataset)
                point = ds[variable].sel(time=slice(start_date, end_date)).sel(longitude=lon,latitude=lat,method='nearest')
            else:
                ds = xr.open_dataset('http://thredds.servirglobal.net/thredds/dodsC/Agg/' + dataset)
                # get the dataset that has sliced data based on dates and geometry
                point = ds[variable].sel(time=slice(start_date,end_date)).sel(longitude=lon,latitude=lat,method='nearest')

            dates=point.time.dt.strftime("%Y-%m-%d").values.tolist()

            # min/max/avg return same values for a point geometry
            values=np.array(point.values)
            values = values[values != -999.0]
        except Exception as e:
            logger.info(e)
        if len(values)==0:
            return [],[]
        return dates, values
    else:
        return [],[],[]

    # delete temporary netCDF file on server if flag is set to True
    if params.deletetempnetcdf == True and os.path.exists(temp_file):
        os.remove(temp_file)

# To retrive the CHIRPS data from 1981  to 2020 for Monthly Analysis.
# Retrieves 25th, 50th, 75th percentiles corresponding to month list from NMME
def get_chirps_climatology(month_nums):
    basepath='http://thredds.servirglobal.net/thredds/dodsC/climateserv/process_tmp/downloads/chirps/ucsb-chirps-monthly-resolved-for-climatology'
    ext='.nc4'
    ds = xr.open_dataset(basepath + ext)

    precip = ds.precipitation_amount.sel(latitude=slice(-5, 5), longitude=slice(25, 30)).mean(
        dim=['latitude', 'longitude'])

    LTA=precip.groupby('time.month').mean(dim='time').values  # this give the mean climatology for the spatial averages
    print('LTA')
    print(LTA)

    value = precip.chunk(dict(time=-1)).groupby('time.month').quantile(q=[0.25, 0.50, 0.75], dim='time').values

    resarr = [list(value[month_nums[0] - 1]), list(value[month_nums[1] - 1]), list(value[month_nums[2] - 1]), list(value[month_nums[3] - 1]),
              list(value[month_nums[4] - 1]), list(value[month_nums[5] - 1])]
    return resarr,LTA

# To retrieve NMME data from start date of the netCDF to 180 days from start date
# Requires bounds of the geometry to get the data from CCSM4 and CFSV2 sensor files.
# Uses first five ensembles from both sensors
def get_nmme_data(total_bounds):
    lon1, lat1, lon2, lat2 = total_bounds
    # set number of ensembles to use from each dataset.
    numEns = 5
    ccsm4 = []
    cfsv2 = []
    LTA=[]
    for iens in np.arange(numEns):
        ccsm4.append(xr.open_dataset(params.nmme_ccsm4_path+'nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens00' + str(iens + 1) + '.nc4').sel(
            longitude=slice(lon1, lon2), latitude=slice(lat1, lat2)).expand_dims(dim={'ensemble': np.array([iens + 1])},
                                                                                 axis=0))
        cfsv2.append(xr.open_dataset(params.nmme_cfsv2_path+'nmme-cfsv2_bcsd.latest.global.0.5deg.daily.ens00' + str(iens + 1) + '.nc4').sel(
            longitude=slice(lon1, lon2), latitude=slice(lat1, lat2)).expand_dims(dim={'ensemble': np.array([iens + 1])},
                                                                                 axis=0))
    cfsv2 = xr.merge(cfsv2)
    ccsm4 = xr.merge(ccsm4)

    # Combine the files from CCSM4 and CSFV2 sensors
    combined=xr.concat([ccsm4,cfsv2.assign_coords(ensemble=cfsv2.ensemble+5) ],dim='ensemble')

    # Retrieves the avg values of precipitation
    return combined.precipitation.resample(time='1M',label='left',loffset='1D').sum().mean(dim=['ensemble','latitude','longitude']).values,LTA

# To retrieve months list and data of CHIRPS/NMME for Monthly Analysis
def get_season_values(type, geom):
    # Get start date and end date for NMME from netCDf file
    nc_file = xr.open_dataset(params.nmme_ccsm4_path+'nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens001.nc4')
    start_date = nc_file["time"].values.min()
    t = pd.to_datetime(str(start_date))
    start_date = t.strftime('%Y-%m-%d')
    ed = datetime.strptime(start_date,'%Y-%m-%d') +  timedelta(days = 180)
    end_date = ed.strftime('%Y-%m-%d')

    month_list = [i.strftime("%Y-%m-%d") for i in pd.date_range(start=start_date, end=end_date, freq='MS')]
    month_nums =[int(i.strftime("%m")) for i in pd.date_range(start=start_date, end=end_date, freq='MS')]

    try:
        print("get_season_values - TRY")
        # add properties to geometry json if it did not exist
        jsonn = json.loads(str(geom))
        for x in range(len(jsonn["features"])):
            if "properties" not in jsonn["features"][x]:
                jsonn["features"][x]["properties"] = {}
        json_aoi = json.dumps(jsonn)
        # using geopandas to get the bounds
        aoi = gpd.read_file(json_aoi)

        # Get CHIRPS 40 year historical data and NMME 180 day forecast
        if type== "chirps":
            data,LTA = get_chirps_climatology(month_nums)
        elif type == "nmme":
            data,LTA = get_nmme_data(aoi.total_bounds)
            print('after nmme')
    except Exception as e:
        logger.info(e)
    return month_list,data, LTA