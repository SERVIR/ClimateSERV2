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
        st = datetime.strptime(start_date, '%m/%d/%Y')
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


def get_season_values(variable, dataset, geom,temp_file, uniqueid):
    nc_file = xr.open_dataset(temp_file)
    start_date = nc_file["time"].values.min()
    t = pd.to_datetime(str(start_date))
    start_date = t.strftime('%Y-%m-%d')
    ed = datetime.strptime(start_date,'%Y-%m-%d') +  timedelta(days = 180)
    end_date = ed.strftime('%Y-%m-%d')
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
    east = aoi.total_bounds[2]
    south = aoi.total_bounds[1]
    west = aoi.total_bounds[0]
    north = aoi.total_bounds[3]
    # Extracting data from TDS and making local copy
    if jsonn['features'][0]['geometry']['type']=="Point":
        dates = []
        try:
            coords = jsonn['features'][0]['geometry']['coordinates']
            lon, lat = coords[0], coords[1]
            try:
                ds=xr.open_dataset('http://thredds.servirglobal.net/thredds/dodsC/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/'+dataset)
            except:
                ds = xr.open_dataset(
                    'http://thredds.servirglobal.net/thredds/dodsC/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/' + dataset)
            point = ds[variable].sel(time=slice(start_date,end_date)).sel(longitude=lon,latitude=lat,method='nearest')
            dates=point.time.dt.strftime("%Y-%m-%d").values.tolist()
        except Exception as e:
            logger.info(e)
        values=np.array(point.values)
        values = values[values != -999.0]
        if len(values)==0:
            return [],[]
        return dates, values
    try:
        tds_request = "http://thredds.servirglobal.net/thredds/ncss/climateserv/nmme-ccsm4_bcsd/global/0.5deg/daily/latest/" + dataset + "?var=" + variable + "&north=" + str(
            north) + "&west=" + str(west) + "&east=" + str(east) + "&south=" + str(
            south) + "&disableProjSubset=on&horizStride=1&time_start=" + str(
            start_date) + "T00%3A00%3A00Z&time_end=" + str(end_date) + "T00%3A00%3A00Z&timeStride=1"
        tf = os.path.join(params.netCDFpath, uniqueid + "_" + dataset)  # name for temporary netcdf file
        urllib.request.urlretrieve(tds_request, tf)
    except:
        tds_request = "http://thredds.servirglobal.net/thredds/ncss/climateserv/nmme-cfsv2_bcsd/global/0.5deg/daily/latest/" + dataset + "?var=" + variable + "&north=" + str(
            north) + "&west=" + str(west) + "&east=" + str(east) + "&south=" + str(
            south) + "&disableProjSubset=on&horizStride=1&time_start=" + str(
            start_date) + "T00%3A00%3A00Z&time_end=" + str(end_date) + "T00%3A00%3A00Z&timeStride=1"
        tf = os.path.join(params.netCDFpath, uniqueid + "_" + dataset)  # name for temporary netcdf file
        urllib.request.urlretrieve(tds_request, tf)

    clipped_dataset = xr.open_dataset(tf)

    avg_values = clipped_dataset.mean(dim=["latitude", "longitude"], skipna=True)
    dates = []
    for i in avg_values[variable]:
        temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
        dates.append(temp.strftime("%Y-%m-%d"))
    vals=avg_values[variable].values
    # vs = vals[vals != np.nan]
    vs = vals[~np.isnan(vals).any(axis=0)]
    if(len(vs)==0):
        return [],[]
    return np.array(dates), vs[0]