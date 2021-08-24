import json
import geopandas as gpd
import urllib
import xarray as xr
from datetime import datetime
from shapely.geometry import mapping
import numpy as np
import pandas as pd
import subprocess
from zipfile import ZipFile
from os.path import basename
import os
try:
    import climateserv2.locallog.locallogging as llog
    import climateserv2.parameters as params

except:
    import locallog.locallogging as llog
    import parameters as params
import shutil
logger=llog.getNamedLogger("request_processor")
def get_aggregated_values(start_date, end_date, dataset, variable, geom, task_id, operation):

    json_aoi = json.loads(geom) # convert coordinates to a json object
    for x in range(len(json_aoi['features'])):
        if "properties" not in json_aoi['features'][x]:
            json_aoi['features'][x]['properties'] = {}

    aoi = gpd.GeoDataFrame.from_features(json_aoi, crs="EPSG:4326")
    aoi.head()
    # aoi = gpd.read_file(json_aoi)  # using geopandas to get the bounds
    # The netcdf files use a global lat/lon so adjust values accordingly


    st=datetime.strptime(start_date, '%m/%d/%Y')
    et=datetime.strptime(end_date, '%m/%d/%Y')
    start_date=datetime.strftime(st, '%Y-%m-%d')
    end_date=datetime.strftime(et, '%Y-%m-%d')


    temp_file = task_id + "_" + dataset  # name for temporary netcdf file
    if json_aoi['features'][0]['geometry']['type']=="Point":
        coords=json_aoi['features'][0]['geometry']['coordinates']
        lat, lon=coords[0],coords[1]
        try:
            tds_request =  "http://thredds.servirglobal.net/thredds/ncss/Agg/" + dataset + "?var=" + variable + "&latitude="+str(lat)+"&longitude="+str(lon)+"&time_start=" + start_date + "T00%3A00%3A00Z&time_end=" + end_date + "T00%3A00%3A00Z&accept=netcdf"
            urllib.request.urlretrieve(tds_request, temp_file)
            clipped_dataset = xr.open_dataset(temp_file)
        except:
            logger.info("thredds URL exception")
    else:
        east = aoi.total_bounds[2]
        south = aoi.total_bounds[1]
        west = aoi.total_bounds[0]
        north = aoi.total_bounds[3]
        tds_request = "http://thredds.servirglobal.net/thredds/ncss/Agg/" + dataset + "?var=" + variable + "&north=" + str(
            north) + "&west=" + str(west) + "&east=" + str(east) + "&south=" + str(
            south) + "&disableProjSubset=on&horizStride=1&time_start=" + start_date + "T00%3A00%3A00Z&time_end=" + end_date + "T00%3A00%3A00Z&timeStride=1"
        urllib.request.urlretrieve(tds_request, temp_file)
        xds = xr.open_dataset(temp_file)  # using xarray to open the temporary netcdf
        xds = xds[[variable]].transpose('time', 'latitude', 'longitude')
        xds.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude", inplace=True)
        xds.rio.write_crs("EPSG:4326", inplace=True)

        clipped_dataset = xds.rio.clip(aoi.geometry, aoi.crs, drop=False, invert=True)  # clip dataset
    os.remove(temp_file)  # delete temporary file on server
    if json_aoi['features'][0]['geometry']['type']=="Point" and operation != "download" :
        dates = []
        for i in np.array(clipped_dataset['time'].values):
            temp = pd.Timestamp(np.datetime64(i)).to_pydatetime()
            dates.append(temp.strftime("%Y-%m-%d"))
        values = np.array(clipped_dataset[variable].values)
        return dates, operation, values, aoi.total_bounds
    elif operation == "min":
        dates = []
        min_values = clipped_dataset.min(dim=["latitude", "longitude"], skipna=True)
        min_values_array=np.array(min_values[variable].values)
        for i in min_values[variable]:
            temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
            dates.append(temp.strftime("%Y-%m-%d"))
        return dates,operation,np.array(min_values_array),aoi.total_bounds
    elif operation == "avg":
        dates = []
        logger.info("in avg")
        mean_values = clipped_dataset.mean(dim=["latitude", "longitude"], skipna=True)
        mean_values_array = np.array(mean_values[variable].values)
        for i in mean_values[variable]:
            temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
            dates.append(temp.strftime("%Y-%m-%d"))
        return dates,operation,np.array(mean_values_array),aoi.total_bounds
    elif operation == "max":
        dates = []
        max_values = clipped_dataset.max(dim=["latitude", "longitude"], skipna=True)
        max_values_array = np.array(max_values[variable].values)
        for i in max_values[variable]:
            temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
            dates.append(temp.strftime("%Y-%m-%d"))
        return dates, operation, np.array(max_values_array), aoi.total_bounds
    elif operation == "download":
        os.makedirs(params.zipFile_ScratchWorkspace_Path+task_id+'/',exist_ok=True)
        os.chmod(params.zipFile_ScratchWorkspace_Path + task_id + '/',0o777)
        os.chmod(params.shell_script, 0o777)
        clipped_dataset.to_netcdf(params.zipFile_ScratchWorkspace_Path+"/"+'clipped_'+dataset)
        os.chdir(params.zipFile_ScratchWorkspace_Path+task_id+'/')
        p = subprocess.check_call([params.shell_script, params.zipFile_ScratchWorkspace_Path+'/clipped_'+dataset , variable, params.zipFile_ScratchWorkspace_Path+task_id+'/'])
        with ZipFile(params.zipFile_ScratchWorkspace_Path+task_id+'.zip', 'w') as zipObj:
            # Iterate over all the files in directory
            for folderName, subfolders, filenames in os.walk(params.zipFile_ScratchWorkspace_Path+task_id+'/'):
                for filename in filenames:
                    # create complete filepath of file in directory
                    filePath = os.path.join(folderName, filename)
                    # Add file to zip
                    zipObj.write(filePath, basename(filePath))

        # close the Zip File
        zipObj.close()
      #  os.remove(params.zipFile_ScratchWorkspace_Path+'/clipped_'+dataset)
        return params.zipFile_ScratchWorkspace_Path+task_id+'.zip',operation
    else:
        return "invalid operation"