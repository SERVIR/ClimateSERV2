import json
import pandas as pd
import geopandas as gpd
import urllib
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
lat, lon = 0, 0
def get_tds_request(start_date, end_date, dataset, variable, geom):
    jsonn = json.loads(str(geom))
    for x in range(len(jsonn["features"])):
        if "properties" not in jsonn["features"][x]:
            jsonn["features"][x]["properties"] = {}
    json_aoi = json.dumps(jsonn)
    try:
        # aoi = gpd.GeoDataFrame.from_features(json_aoi, crs="EPSG:4326")
        aoi = gpd.read_file(json_aoi)  # using geopandas to get the bounds
    except Exception as e:
        logger.info(e)
    try:
        st = datetime.strptime(start_date, '%m/%d/%Y')
        et = datetime.strptime(end_date, '%m/%d/%Y')
        start_date = datetime.strftime(st, '%Y-%m-%d')
        end_date = datetime.strftime(et, '%Y-%m-%d')
    except:
        start_date = start_date
        end_date = end_date
    if jsonn['features'][0]['geometry']['type'] == "Point":
        count = 9
        coords = jsonn['features'][0]['geometry']['coordinates']
        lat, lon = coords[0], coords[1]
        tds_request = "http://thredds.servirglobal.net/thredds/ncss/Agg/" + dataset + "?var=" + variable + "&latitude=" + str(
            lat) + "&longitude=" + str(
            lon) + "&time_start=" + start_date + "T00%3A00%3A00Z&time_end=" + end_date + "T00%3A00%3A00Z&accept=netcdf"
        logger.info(tds_request)
        #tds_request ='http://thredds.servirglobal.net/thredds/dodsC/Agg/ucsb-chirps_global_0.05deg_daily.nc4'
    else:
        # The netcdf files use a global lat/lon so adjust values accordingly
        east = aoi.total_bounds[2]
        south = aoi.total_bounds[1]
        west = aoi.total_bounds[0]
        north = aoi.total_bounds[3]
        # Extracting data from TDS and making local copy
        tds_request = "http://thredds.servirglobal.net/thredds/ncss/Agg/" + dataset + "?var=" + variable + "&north=" + str(
            north) + "&west=" + str(west) + "&east=" + str(east) + "&south=" + str(
            south) + "&disableProjSubset=on&horizStride=1&time_start=" + start_date + "T00%3A00%3A00Z&time_end=" + end_date + "T00%3A00%3A00Z&timeStride=1"
    return tds_request

def get_aggregated_values(start_date, end_date, dataset, variable, geom, operation, temp_file):
    try:
        # st = datetime.strptime(start_date, '%m/%d/%Y')
        et = datetime.strptime(end_date, '%m/%d/%Y')
        start_date = datetime.strftime(st, '%Y-%m-%d')
        end_date = datetime.strftime(et, '%Y-%m-%d')
    except:
        start_date = start_date
        end_date = end_date
    jsonn =json.loads(str(geom))
    for x in range(len(jsonn["features"])):
        if "properties" not in jsonn["features"][x]:
            jsonn["features"][x]["properties"] = {}
    json_aoi=json.dumps(jsonn)
    if jsonn['features'][0]['geometry']['type']!="Point" and os.path.exists(temp_file):

        xds = xr.open_dataset(temp_file)  # using xarray to open the temporary netcdf

        xds = xds[[variable]].transpose('time', 'latitude', 'longitude')

        xds.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude", inplace=True)

        xds.rio.write_crs("EPSG:4326", inplace=True)

        geodf = gpd.read_file(json_aoi)

        clipped_dataset = xds.rio.clip(geodf.geometry.apply(mapping), geodf.crs)

        time_var = xds[[variable]].time
        new_data = []

        for x in time_var:
            temp = pd.Timestamp(np.datetime64(x.time.values)).to_pydatetime()
            new_data.append(temp.strftime("%Y-%m-%d"))
        if operation == "min":
            min_values = clipped_dataset.min(dim=["latitude", "longitude"], skipna=True)
            dates = []
            for i in min_values[variable]:
                temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
                dates.append(temp.strftime("%Y-%m-%d"))
            return np.array(dates), np.array(min_values[variable].values)
        elif operation == "avg":
            avg_values = clipped_dataset.mean(dim=["latitude", "longitude"], skipna=True)
            dates = []
            for i in avg_values[variable]:
                temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
                dates.append(temp.strftime("%Y-%m-%d"))
            return np.array(dates), np.array(avg_values[variable].values)
        elif operation == "max":
            max_values = clipped_dataset.max(dim=["latitude", "longitude"], skipna=True)
            dates = []
            for i in max_values[variable]:
                temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
                dates.append(temp.strftime("%Y-%m-%d"))
            return np.array(dates), np.array(max_values[variable].values)
        elif operation == "download":
            return clipped_dataset
    elif jsonn['features'][0]['geometry']['type']=="Point":
        dates = []
        try:
            coords = jsonn['features'][0]['geometry']['coordinates']
            lon, lat = coords[0], coords[1]
            ds=xr.open_dataset('http://thredds.servirglobal.net/thredds/dodsC/Agg/'+dataset)
            point = ds[variable].sel(time=slice(start_date,end_date)).sel(longitude=lon,latitude=lat,method='nearest')
            dates=point.time.dt.strftime("%Y-%m-%d").values.tolist()
        except Exception as e:
            logger.info(e)
        values=np.array(point.values)
        values = values[values != -999.0]
        if len(values)==0:
            return [],[]
        return dates, values
    else:
        return [],[],[]

    if params.deletetempnetcdf == True and os.path.exists(temp_file):
        os.remove(temp_file)  # delete temporary file on server


def get_chirps_climatology(month_nums):
    vals = []
    basepath='http://thredds.servirglobal.net/thredds/dodsC/climateserv/process_tmp/downloads/chirps/ucsb-chirps-monthly-resolved-for-climatology'
    ext='.nc4'
    ds = xr.open_dataset(basepath + ext)

    precip = ds.precipitation_amount.sel(latitude=slice(-5, 5), longitude=slice(25, 30)).mean(
        dim=['latitude', 'longitude'])

    precip.groupby('time.month').mean(dim='time').values  # this give the mean climatology for the spatial averages

    value = precip.chunk(dict(time=-1)).groupby('time.month').quantile(q=[0.25, 0.50, 0.75], dim='time').values

    resarr = [list(value[month_nums[0] - 1]), list(value[month_nums[1] - 1]), list(value[month_nums[2] - 1]), list(value[month_nums[3] - 1]),
              list(value[month_nums[4] - 1]), list(value[month_nums[5] - 1])]
    return resarr


def get_nmme_data(total_bounds):
    lon1, lat1, lon2, lat2 = total_bounds
    numEns = 5  # set number of ensembles to use from each dataset.
    ccsm4 = []
    cfsv2 = []
    for iens in np.arange(numEns):

        ccsm4.append(xr.open_dataset(
            '/mnt/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens00' + str(iens + 1) + '.nc4').sel(
            longitude=slice(lon1, lon2), latitude=slice(lat1, lat2)).expand_dims(dim={'ensemble': np.array([iens + 1])},
                                                                                 axis=0))
        cfsv2.append(xr.open_dataset(
            '/mnt/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/nmme-cfsv2_bcsd.latest.global.0.5deg.daily.ens00' + str(iens + 1) + '.nc4').sel(
            longitude=slice(lon1, lon2), latitude=slice(lat1, lat2)).expand_dims(dim={'ensemble': np.array([iens + 1])},
                                                                                 axis=0))
    cfsv2 = xr.merge(cfsv2)
    ccsm4 = xr.merge(ccsm4)
    combined=xr.concat([ccsm4,cfsv2.assign_coords(ensemble=cfsv2.ensemble+5) ],dim='ensemble')




    return combined.precipitation.resample(time='1M',label='left',loffset='1D').sum().mean(dim=['ensemble','latitude','longitude']).values



def get_season_values( type, geom, uniqueid):

    nc_file = xr.open_dataset('/mnt/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens001.nc4')
    start_date = nc_file["time"].values.min()
    t = pd.to_datetime(str(start_date))
    start_date = t.strftime('%Y-%m-%d')
    ed = datetime.strptime(start_date,'%Y-%m-%d') +  timedelta(days = 180)
    end_date = ed.strftime('%Y-%m-%d')
    month_list = [i.strftime("%Y-%m-%d") for i in pd.date_range(start=start_date, end=end_date, freq='MS')]
    month_nums =[int(i.strftime("%m")) for i in pd.date_range(start=start_date, end=end_date, freq='MS')]
    logger.info("%%%%%")
    logger.info(month_nums)
    jsonn = json.loads(str(geom))
    for x in range(len(jsonn["features"])):
        if "properties" not in jsonn["features"][x]:
            jsonn["features"][x]["properties"] = {}
    json_aoi = json.dumps(jsonn)
    try:
        # aoi = gpd.GeoDataFrame.from_features(json_aoi, crs="EPSG:4326")
        aoi = gpd.read_file(json_aoi)  # using geopandas to get the bounds
    except Exception as e:
        logger.info(e)
    if type== "chirps":
        data = get_chirps_climatology(month_nums)
        logger.info(data)
    elif type == "nmme":
        logger.info("nmmee")
        data = get_nmme_data(aoi.total_bounds)
        logger.info(data)
    logger.info(month_list)
    return month_list,data
    # # Extracting data from TDS and making local copy
    # if jsonn['features'][0]['geometry']['type']=="Point":
    #     dates = []
    #     try:
    #         coords = jsonn['features'][0]['geometry']['coordinates']
    #         lon, lat = coords[0], coords[1]
    #         try:
    #             ds=xr.open_dataset('http://thredds.servirglobal.net/thredds/dodsC/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/'+dataset)
    #         except:
    #             ds = xr.open_dataset(
    #                 'http://thredds.servirglobal.net/thredds/dodsC/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/' + dataset)
    #         point = ds[variable].sel(time=slice(start_date,end_date)).sel(longitude=lon,latitude=lat,method='nearest')
    #         dates=point.time.dt.strftime("%Y-%m-%d").values.tolist()
    #     except Exception as e:
    #         logger.info(e)
    #     values=np.array(point.values)
    #     values = values[values != -999.0]
    #     if len(values)==0:
    #         return [],[]
    #     return dates, values
