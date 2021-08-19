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

logger=llog.getNamedLogger("request_processor")
def get_aggregated_values(start_date, end_date, dataset, variable, geom, task_id, operation,outFileFolder=''):
    json_aoi = json.dumps(json.loads(geom))  # convert coordinates to a json object

    aoi = gpd.read_file(json_aoi)  # using geopandas to get the bounds
    # The netcdf files use a global lat/lon so adjust values accordingly
    east = aoi.total_bounds[2]
    south = aoi.total_bounds[1]
    west = aoi.total_bounds[0]
    north = aoi.total_bounds[3]

    st=datetime.strptime(start_date, '%m/%d/%Y')
    et=datetime.strptime(end_date, '%m/%d/%Y')
    start_date=datetime.strftime(st, '%Y-%m-%d')
    end_date=datetime.strftime(et, '%Y-%m-%d')

    tds_request = "http://thredds.servirglobal.net/thredds/ncss/Agg/" + dataset + "?var=" + variable + "&north=" + str(
        north) + "&west=" + str(west) + "&east=" + str(east) + "&south=" + str(
        south) + "&disableProjSubset=on&horizStride=1&time_start=" + start_date + "T00%3A00%3A00Z&time_end=" + end_date + "T00%3A00%3A00Z&timeStride=1"
    temp_file = task_id + "_" + dataset  # name for temporary netcdf file
    urllib.request.urlretrieve(tds_request, temp_file)  # download netcdf to a file on server
    xds = xr.open_dataset(temp_file)  # using xarray to open the temporary netcdf
    xds = xds[[variable]].transpose('time', 'latitude', 'longitude')
    xds.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude", inplace=True)
    xds.rio.write_crs("EPSG:4326", inplace=True)
    os.remove(temp_file)  # delete temporary file on server

    geodf = gpd.read_file(json_aoi)

    clipped_dataset = xds.rio.clip(geodf.geometry.apply(mapping), geodf.crs)  # clip dataset to polygon
    # calculate values based on operation

    if operation == "min":
        dates = []
        min_values = clipped_dataset.min(dim=["latitude", "longitude"], skipna=True)
        for i in min_values[variable]:
            temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
            dates.append(temp.strftime("%Y-%m-%d"))
        return dates,operation,np.array(min_values[variable].values)
    elif operation == "avg":
        dates = []
        mean_values = clipped_dataset.mean(dim=["latitude", "longitude"], skipna=True)
        for i in mean_values[variable]:
            temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
            dates.append(temp.strftime("%Y-%m-%d"))
        return dates,operation,np.array(mean_values[variable].values)
    elif operation == "max":
        dates = []
        max_values = clipped_dataset.max(dim=["latitude", "longitude"], skipna=True)
        for i in max_values[variable]:
            temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
            dates.append(temp.strftime("%Y-%m-%d"))
        return dates,operation,np.array(max_values[variable].values)
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
        os.remove(params.zipFile_ScratchWorkspace_Path+'/clipped_'+dataset)
        return params.zipFile_ScratchWorkspace_Path+task_id+'.zip',operation
    else:
        return "invalid operation"