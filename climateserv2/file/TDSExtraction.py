import json
import pandas as pd
import geopandas as gpd
import urllib
import numpy as np
import os
import xarray as xr
from datetime import datetime
from shapely.geometry import mapping, Polygon,Point
import rioxarray
import shutil
try:
    import climateserv2.locallog.locallogging as llog
    import climateserv2.parameters as params

except:
    import locallog.locallogging as llog
    import parameters as params
import subprocess
logger=llog.getNamedLogger("request_processor")

def get_polygon_from_point(lon, lat):
    pt = Point(lon,lat)
    # buffer with CAP_STYLE = 3
    buf = pt.buffer(10, cap_style=3)
    polygon = gpd.GeoSeries([buf]).__geo_interface__
    return polygon

def get_aggregated_values(start_date, end_date, dataset, variable, geom, task_id, operation):
    percent=0
    count = 0
    jsonn =json.loads(str(geom))
    for x in range(len(jsonn["features"])):
        if "properties" not in jsonn["features"][x]:
            jsonn["features"][x]["properties"] = {}
    json_aoi=json.dumps(jsonn)

    try:
        # aoi = gpd.GeoDataFrame.from_features(json_aoi, crs="EPSG:4326")
        aoi = gpd.read_file(json_aoi) # using geopandas to get the bounds
    except Exception as e:
        logger.info(e)

    st = datetime.strptime(start_date, '%m/%d/%Y')
    et = datetime.strptime(end_date, '%m/%d/%Y')
    start_date = datetime.strftime(st, '%Y-%m-%d')
    end_date = datetime.strftime(et, '%Y-%m-%d')
    if jsonn['features'][0]['geometry']['type']=="Point":
        logger.info("its a poitn")
        count=9
        coords=jsonn['features'][0]['geometry']['coordinates']
        lat, lon=coords[0],coords[1]
        polygon = get_polygon_from_point(lon,lat)
        east = list(polygon['bbox'])[2]
        south = list(polygon['bbox'])[1]
        west = list(polygon['bbox'])[0]
        north = list(polygon['bbox'])[3]
        try:
            tds_request =  "http://thredds.servirglobal.net/thredds/ncss/Agg/" + dataset + "?var=" + variable + "&latitude="+str(lat)+"&longitude="+str(lon)+"&time_start=" + start_date + "T00%3A00%3A00Z&time_end=" + end_date + "T00%3A00%3A00Z&accept=netcdf"
            temp_file = os.path.join(params.netCDFpath, task_id + "_" + dataset)  # name for temporary netcdf file

            urllib.request.urlretrieve(tds_request, temp_file)

            clipped_dataset = xr.open_dataset(temp_file)

        except:
            logger.info("thredds URL exception")
    else:
        logger.info('from else')

        # The netcdf files use a global lat/lon so adjust values accordingly
        east = aoi.total_bounds[2]
        south = aoi.total_bounds[1]
        west = aoi.total_bounds[0]
        north = aoi.total_bounds[3]



        # Extracting data from TDS and making local copy
        tds_request = "http://thredds.servirglobal.net/thredds/ncss/Agg/" + dataset + "?var=" + variable + "&north=" + str(
            north) + "&west=" + str(west) + "&east=" + str(east) + "&south=" + str(
            south) + "&disableProjSubset=on&horizStride=1&time_start=" + start_date + "T00%3A00%3A00Z&time_end=" + end_date + "T00%3A00%3A00Z&timeStride=1"
        temp_file = os.path.join(params.netCDFpath, task_id + "_" + dataset)  # name for temporary netcdf file
        logger.info(tds_request)
        try:
            urllib.request.urlretrieve(tds_request, temp_file)

        except:
            logger.info("thredds URL exception")

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
        print(new_data)
        if (start_date not in new_data) and (end_date not in new_data):
            count = 1
    if count == 0 and os.path.exists(temp_file):
        percent=20
        if operation == "min":
            min_values = clipped_dataset.min(dim=["latitude", "longitude"], skipna=True)
            dates = []
            for i in min_values[variable]:
                temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
                dates.append(temp.strftime("%Y-%m-%d"))
            return np.array(dates), np.array(min_values[variable].values),percent
        elif operation == "avg":
            avg_values = clipped_dataset.mean(dim=["latitude", "longitude"], skipna=True)
            dates = []
            for i in avg_values[variable]:
                temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
                dates.append(temp.strftime("%Y-%m-%d"))
            return np.array(dates), np.array(avg_values[variable].values),percent
        elif operation == "max":
            logger.info('from max')
            max_values = clipped_dataset.max(dim=["latitude", "longitude"], skipna=True)
            dates = []
            for i in max_values[variable]:
                temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
                dates.append(temp.strftime("%Y-%m-%d"))
            print('fdhfj')
            return np.array(dates), np.array(max_values[variable].values),percent
        elif operation == "download":
            return clipped_dataset,task_id,percent
    elif count==9 and os.path.exists(temp_file):
        dates = []
        for i in np.array(clipped_dataset['time'].values):
            temp = pd.Timestamp(np.datetime64(i)).to_pydatetime()
            dates.append(temp.strftime("%Y-%m-%d"))
        values = np.array(clipped_dataset[variable].values)
        return dates, values,percent
    else:
        return [],[],[]

    if params.deletetempnetcdf == True:
        os.remove(temp_file)  # delete temporary file on server
