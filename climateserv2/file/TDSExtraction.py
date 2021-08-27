import json
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import urllib
import numpy as np
import os
import xarray as xr
from datetime import datetime
from shapely.geometry import mapping, Polygon
import rioxarray
from zipfile import ZipFile
from os.path import basename
import netCDF4
import shutil
try:
    import climateserv2.locallog.locallogging as llog
    import climateserv2.parameters as params

except:
    import locallog.locallogging as llog
    import parameters as params
import subprocess
logger=llog.getNamedLogger("request_processor")

def get_aggregated_values(start_date, end_date, dataset, variable, geom, task_id, operation):
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

    # aoi.head()
    #print(aoi.total_bounds)

    # The netcdf files use a global lat/lon so adjust values accordingly
    east = aoi.total_bounds[2]
    south = aoi.total_bounds[1]
    west = aoi.total_bounds[0]
    north = aoi.total_bounds[3]

    st=datetime.strptime(start_date, '%m/%d/%Y')
    et=datetime.strptime(end_date, '%m/%d/%Y')
    start_date=datetime.strftime(st, '%Y-%m-%d')
    end_date=datetime.strftime(et, '%Y-%m-%d')
    # Extracting data from TDS and making local copy
    tds_request = "http://thredds.servirglobal.net/thredds/ncss/Agg/" + dataset + "?var=" + variable + "&north=" + str(
        north) + "&west=" + str(west) + "&east=" + str(east) + "&south=" + str(
        south) + "&disableProjSubset=on&horizStride=1&time_start=" + start_date + "T00%3A00%3A00Z&time_end=" + end_date + "T00%3A00%3A00Z&timeStride=1"
    #logger.info(tds_request)
    temp_file = os.path.join(params.netCDFpath, task_id + "_" + dataset)  # name for temporary netcdf file

    #print("Download Started =", datetime.now().strftime("%H:%M:%S"))
    urllib.request.urlretrieve(tds_request, temp_file)
    #print("Download Ended =", datetime.now().strftime("%H:%M:%S"))
    # Reading local NetCDF
    #print("Current Time =", datetime.now().strftime("%H:%M:%S"))

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
    if count == 0:
        if operation == "min":
            min_values = clipped_dataset.min(dim=["latitude", "longitude"], skipna=True)
            dates = []
            for i in min_values[variable]:
                temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
                dates.append(temp.strftime("%Y-%m-%d"))
            return np.array(dates), operation, np.array(min_values[variable].values), aoi.total_bounds
        elif operation == "avg":
            avg_values = clipped_dataset.mean(dim=["latitude", "longitude"], skipna=True)
            dates = []
            for i in avg_values[variable]:
                temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
                dates.append(temp.strftime("%Y-%m-%d"))
            return np.array(dates), operation, np.array(avg_values[variable].values), aoi.total_bounds
        elif operation == "max":
            max_values = clipped_dataset.max(dim=["latitude", "longitude"], skipna=True)
            dates = []
            for i in max_values[variable]:
                temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
                dates.append(temp.strftime("%Y-%m-%d"))
            return np.array(dates),operation, np.array(max_values[variable].values), aoi.total_bounds
        elif operation == "download":
            os.makedirs(params.zipFile_ScratchWorkspace_Path + task_id + '/', exist_ok=True)
            os.chmod(params.zipFile_ScratchWorkspace_Path + task_id + '/', 0o777)
            os.chmod(params.shell_script, 0o777)
            clipped_dataset.to_netcdf(params.zipFile_ScratchWorkspace_Path + "/" + 'clipped_' + dataset)
            os.chdir(params.zipFile_ScratchWorkspace_Path + task_id + '/')
            p = subprocess.check_call(
                [params.shell_script, params.zipFile_ScratchWorkspace_Path + '/clipped_' + dataset, variable,
                 params.zipFile_ScratchWorkspace_Path + task_id + '/'])
            with ZipFile(params.zipFile_ScratchWorkspace_Path + task_id + '.zip', 'w') as zipObj:
                # Iterate over all the files in directory
                for folderName, subfolders, filenames in os.walk(params.zipFile_ScratchWorkspace_Path + task_id + '/'):
                    for filename in filenames:
                        # create complete filepath of file in directory
                        filePath = os.path.join(folderName, filename)
                        # Add file to zip
                        zipObj.write(filePath, basename(filePath))

            # close the Zip File
            zipObj.close()
            os.remove(params.zipFile_ScratchWorkspace_Path + '/clipped_' + dataset)
            shutil.rmtree(params.zipFile_ScratchWorkspace_Path + str(task_id), ignore_errors=True)
            return params.zipFile_ScratchWorkspace_Path + task_id + '.zip', operation
        else:
            return "invalid operation"

    if params.deletetempnetcdf == True:
        os.remove(temp_file)  # delete temporary file on server
