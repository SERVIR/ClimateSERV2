import json
import geopandas as gpd
import urllib
import xarray as xr
from datetime import datetime
from shapely.geometry import mapping
import numpy as np
import pandas as pd
try:
    import climateserv2.locallog.locallogging as llog
except:
    import locallog.locallogging as llog
logger=llog.getNamedLogger("request_processor")
def get_aggregated_values(start_date, end_date, dataset, variable, coordinates, task_id, operation):
    json_aoi = json.dumps({"type": "Polygon", "coordinates": coordinates})  # convert coordinates to a json object

    aoi = gpd.read_file(json_aoi)  # using geopandas to get the bounds
    aoi.head()

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

    geodf = gpd.read_file(json_aoi)

    clipped_dataset = xds.rio.clip(geodf.geometry.apply(mapping), geodf.crs)  # clip dataset to polygon

    prcp_amount_values = clipped_dataset.variables[variable]  # the value from the variables passed to the clipped_dataset

    # calculate values based on operation

    if operation == "min":
        dates = []
        min_values = clipped_dataset.min(dim=["latitude", "longitude"], skipna=True)
        return np.array(dates),np.array(min_values)
    elif operation == "avg":
        dates = []
        mean_values = clipped_dataset.mean(dim=["latitude", "longitude"], skipna=True)
        return np.array(dates),np.array(mean_values)
    elif operation == "max":
        dates = []
        max_values = clipped_dataset.max(dim=["latitude", "longitude"], skipna=True)
        for i in max_values[variable]:
            temp = pd.Timestamp(np.datetime64(i.time.values)).to_pydatetime()
            dates.append(temp.strftime("%Y-%m-%d"))
        logger.info("^^^^^^^^^^^^!!")
        logger.info(dates)
        logger.info(np.array(max_values[variable].values))
        return dates,operation,np.array(max_values[variable].values)
    else:
        return "invalid operation"

    os.remove(temp_file)  # delete temporary file on server

# make a call to the method that returns the min/max/avg values as an array based on the operation.
#result_values = get_aggregated_values(start_date, end_date, dataset, variable, coordinates, task_id, operation)
print("Processing is complete.")