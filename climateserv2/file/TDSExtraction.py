import json
from os.path import basename

import pandas as pd
import geopandas as gpd
import numpy as np
import xarray as xr
import rasterio as rio
import csv
from zipfile import ZipFile
from datetime import datetime, timedelta
import os
import shutil
from django import db
from api.models import ETL_Dataset
from api.models import Parameters

try:
    import climateserv2.locallog.locallogging as llog

except:
    import locallog.locallogging as llog

logger = llog.getNamedLogger("request_processor")
params = Parameters.objects.first()


def get_filelist(datatype, start_date, end_date):
    # db.connections.close_all()
    try:
        working_dataset = ETL_Dataset.objects.filter(number=int(datatype)).first()
    except Exception as e:
        print("failed to get dataset in get_filelist: " + e)

    try:
        dataset_name_format = working_dataset.dataset_name_format
    except Exception as e:
        print("failed to get name format in get_filelist" + e)
    final_load_dir = working_dataset.final_load_dir
    dataset_nc4_variable_name = working_dataset.dataset_nc4_variable_name
    # params = Parameters.objects.first()
    year_nums = range(datetime.strptime(start_date, '%Y-%m-%d').year, datetime.strptime(end_date, '%Y-%m-%d').year + 1)
    filelist = []
    dataset_name = dataset_name_format.split('_')
    final_load_dir = working_dataset.fast_directory_path
    if not os.path.exists(final_load_dir):
        os.makedirs(final_load_dir)
    if "ucsb-chirps" == dataset_name[0]:
        for year in year_nums:
            name = final_load_dir + "ucsb_chirps" + ".global." + dataset_name[
                2] + ".daily." + str(year) + ".nc4"
            if os.path.exists(name):
                filelist.append(name)

    elif "ucsb-chirp" == dataset_name[0]:
        for year in year_nums:
            for month in range(12):
                name = final_load_dir + "ucsb_chirp" + ".global." + dataset_name[
                    2] + ".daily." + str(year) + str('{:02d}'.format(month + 1)) + ".nc4"
                if os.path.exists(name):
                    filelist.append(name)
    elif "ucsb-chirps-gefs" == dataset_name[0]:
        for year in year_nums:
            for month in range(12):
                name = final_load_dir \
                       + "ucsb-chirps-gefs" + ".global." + dataset_name[2] \
                       + ".10dy." + str(year) + str('{:02d}'.format(month + 1)) + ".nc4"
                if os.path.exists(name):
                    filelist.append(name)
    elif "usda-smap" == dataset_name[0]:
        for year in year_nums:
            name = final_load_dir \
                   + dataset_name[0] + ".global." + dataset_name[2] \
                   + ".3dy." + str(year) + ".nc4"
            if os.path.exists(name):
                filelist.append(name)
    elif "nmme-ccsm4" == dataset_name[0]:
        name = final_load_dir
        if os.path.exists(name):
            filelist.append(name)
    elif "nmme-cfsv2" == dataset_name[0]:
        name = final_load_dir
        if os.path.exists(name):
            filelist.append(name)
    elif "imerg" in dataset_name[0]:
        for year in year_nums:
            name = final_load_dir + dataset_name[0] \
                   + ".global." + dataset_name[2] \
                   + ".1dy." + str(year) + ".nc4"
            if os.path.exists(name):
                filelist.append(name)
    elif "sport-esi" in dataset_name and "12wk" in dataset_name:
        for year in year_nums:
            name = final_load_dir + dataset_name[0] + ".global." + dataset_name[
                2] + ".12wk." + str(year) + ".nc4"
            if os.path.exists(name):
                filelist.append(name)
    elif "sport-esi" in dataset_name and "4wk" in dataset_name:
        for year in year_nums:
            name = final_load_dir + dataset_name[0] \
                   + ".global." + dataset_name[2] + ".4wk." + str(year) + ".nc4"
            if os.path.exists(name):
                filelist.append(name)
    else:
        if "ndvi" in dataset_name[0]:
            for year in year_nums:
                for month in range(12):
                    name = final_load_dir + dataset_name[0] + "." + dataset_name[
                        1] + ".250m.10dy." + str(year) + str('{:02d}'.format(month + 1)) + ".nc4"
                    if os.path.exists(name):
                        filelist.append(name)
    return filelist, dataset_nc4_variable_name


# To get the dates and values corresponding to the dataset, variable, dates, operation and geometry
def get_thredds_values(uniqueid, start_date, end_date, variable, geom, operation, file_list):
    # Convert dates to %Y-%m-%d format for THREDDS URL
    logger.debug("Made it to get_thredds_values for: " + uniqueid)
    # try:
    #     params = Parameters.objects.first()
    #     logger.debug("got parameters without closing connections")
    # except Exception as e:
    #     logger.debug("Hit exception getting parameters: " + e)
    #     db.connections.close_all()
    #     logger.debug("closed connections")
    #     params = Parameters.objects.first()
    try:
        st = datetime.strptime(start_date, '%m/%d/%Y')
        et = datetime.strptime(end_date, '%m/%d/%Y')
        start_date = datetime.strftime(st, '%Y-%m-%d')
        end_date = datetime.strftime(et, '%Y-%m-%d')
    except:
        # If there is an exception with date format while converting, we can just ignore the conversion and use the
        # passed dates
        pass
    logger.debug("past datetime stuff for: " + uniqueid)
    try:
        jsonn = json.loads(str(geom))
    except Exception as e:
        jsonn = json.loads(json.dumps(geom))
    logger.debug("past json loads  for: " + uniqueid)
    for x in range(len(jsonn["features"])):
        if "properties" not in jsonn["features"][x]:
            jsonn["features"][x]["properties"] = {}

    logger.debug("past adding feature  for: " + uniqueid)
    # If the geometry is not a point, map the area of interest to the netCDF and extract values
    # If the geometry is a point, get dates and values from the openDAP URL as shown below (line 120)
    json_aoi = json.dumps(jsonn)
    logger.debug("past json dump  for: " + uniqueid)
    geodf = gpd.read_file(json_aoi)
    logger.debug("past reading it for: " + uniqueid)
    lon1, lat1, lon2, lat2 = geodf.total_bounds
    # using xarray to open the temporary netcdf
    logger.debug("about to xarray open the data for: " + uniqueid)
    try:
        nc_file = xr.open_mfdataset(file_list, chunks={'time': 16, 'longitude': 256, 'latitude': 256})
    except Exception as e:
        logger.error("open_mfdataset error: " + str(e) + " for: " + uniqueid)
        return [], []
    lat_bounds = nc_file.sel(latitude=[lat1, lat2], method='nearest').latitude.values
    lon_bounds = nc_file.sel(longitude=[lon1, lon2], method='nearest').longitude.values
    latSlice = slice(lat_bounds[0], lat_bounds[1])
    lonSlice = slice(lon_bounds[0], lon_bounds[1])


    data = nc_file[variable].sel(longitude=lonSlice, latitude=latSlice).sel(time=slice(start_date, end_date))

    dates = data.time.dt.strftime("%Y-%m-%d").values.tolist()
    if operation == "min":
        ds_vals = data.min(dim=['latitude', 'longitude']).values
        ds_vals[np.isnan(ds_vals)] = -9999
        return dates, ds_vals
    elif operation == "avg":
        logger.debug("in operation for: " + uniqueid)
        ds_vals = data.mean(dim=['latitude', 'longitude']).values
        ds_vals[np.isnan(ds_vals)] = -9999
        logger.debug("will be returning from avg operation")
        return dates, ds_vals
    elif operation == "max":
        ds_vals = data.max(dim=['latitude', 'longitude']).values
        ds_vals[np.isnan(ds_vals)] = -9999
        return dates, ds_vals
    elif operation == "netcdf":
        try:
            data.to_netcdf(params.zipFile_ScratchWorkspace_Path + uniqueid + '.nc')
            with ZipFile(params.zipFile_ScratchWorkspace_Path + uniqueid + '.zip', 'w') as zipObj:
                zipObj.write(params.zipFile_ScratchWorkspace_Path + uniqueid + '.nc',
                             uniqueid + '.nc')
                zipObj.close()
        except Exception as e:
            logger.error("to_netcdf or zip error: " + str(e))
    elif operation == "download" or operation == "csv":
        if jsonn['features'][0]['geometry']['type'] == "Point":
            values = data.values
            values[np.isnan(values)] = -9999
            keylist = ["Date", "Value"]
            dct = {}
            for ind in range(len(dates)):
                dct[dates[ind]] = values[ind]
            with open(params.zipFile_ScratchWorkspace_Path + uniqueid + '.csv', "w") as file:
                outfile = csv.DictWriter(file, fieldnames=keylist)
                outfile.writeheader()
                if len(dates) > 0:
                    for k, v in dct.items():
                        outfile.writerow({"Date": k, "Value": v[0][0]})
                else:
                    outfile.writerow({"Date": "No data", "Value": "No data"})

            zipFilePath = params.zipFile_ScratchWorkspace_Path + uniqueid + '.csv'
        else:
            files = [write_to_tiff(data.sel(time=[x]), uniqueid) for x in data.time.values]
            if len(files) > 0:
                try:
                    with ZipFile(params.zipFile_ScratchWorkspace_Path + uniqueid + '.zip', 'w') as zipObj:
                        # Iterate over all the files in directory
                        for folderName, subfolders, filenames in os.walk(
                                params.zipFile_ScratchWorkspace_Path + uniqueid + '/'):
                            for filename in filenames:
                                # create complete filepath of file in directory
                                filePath = os.path.join(folderName, filename)
                                # Add file to zip
                                zipObj.write(filePath, basename(filePath))

                    # close the Zip File
                    zipObj.close()
                except Exception as e:
                    logger.error(str(e))
                zipFilePath = params.zipFile_ScratchWorkspace_Path + uniqueid + '.zip'
            else:
                zipFilePath = ""
        return zipFilePath


# To retrive the CHIRPS data from 1981  to 2020 for Monthly Analysis.
# Retrieves 25th, 50th, 75th percentiles corresponding to month list from NMME
def get_chirps_climatology(month_nums, total_bounds):
    basepath = '/mnt/climateserv/process_tmp/downloads/chirps/ucsb-chirps-monthly-resolved-for-climatology.nc4'
    ds = xr.open_dataset(basepath, chunks={'time': 12, 'longitude': 128, 'latitude': 128})
    lon1, lat1, lon2, lat2 = total_bounds
    lat_bounds = ds.sel(latitude=[lat1, lat2], method='nearest').latitude.values
    lon_bounds = ds.sel(longitude=[lon1, lon2], method='nearest').longitude.values
    latSlice = slice(lat_bounds[0], lat_bounds[1])
    lonSlice = slice(lon_bounds[0], lon_bounds[1])
    precip = ds.precipitation_amount.sel(longitude=lonSlice, latitude=latSlice).mean(dim=['latitude', 'longitude'])
    LTA = precip.groupby('time.month').mean(
        dim='time').values  # this give the mean climatology for the spatial averages
    LTA[np.isnan(LTA)] = -9999
    value = precip.chunk(dict(time=-1)).groupby('time.month').quantile(q=[0.25, 0.50, 0.75], dim='time').values
    value[np.isnan(value)] = -9999
    resarr = [list(value[month_nums[0] - 1]), list(value[month_nums[1] - 1]), list(value[month_nums[2] - 1]),
              list(value[month_nums[3] - 1]),
              list(value[month_nums[4] - 1]), list(value[month_nums[5] - 1])]
    LTAarr = [LTA[month_nums[0] - 1], LTA[month_nums[1] - 1], LTA[month_nums[2] - 1], LTA[month_nums[3] - 1],
              LTA[month_nums[4] - 1], LTA[month_nums[5] - 1]]
    return resarr, LTAarr


# To retrieve NMME data from start date of the netCDF to 180 days from start date
# Requires bounds of the geometry to get the data from CCSM4 and CFSV2 sensor files.
# Uses first five ensembles from both sensors
def get_nmme_data(total_bounds):
    # set number of ensembles to use from each dataset.
    numEns = 5
    LTA = []
    for iens in np.arange(numEns):
        lon1, lat1, lon2, lat2 = total_bounds
    basepath = '/mnt/climateserv/process_tmp/fast_nmme_monthly/nmme-mme_bcsd.latest.global.0.5deg.daily.nc4'
    ds = xr.open_dataset(basepath, chunks={'time': 7, 'longitude': 256, 'latitude': 256})
    lat_bounds = ds.sel(latitude=[lat1, lat2], method='nearest').latitude.values
    lon_bounds = ds.sel(longitude=[lon1, lon2], method='nearest').longitude.values
    latSlice = slice(lat_bounds[0], lat_bounds[1])
    lonSlice = slice(lon_bounds[0], lon_bounds[1])
    try:
        nmme_values = ds.precipitation.sel(longitude=lonSlice, latitude=latSlice).mean(
            dim=['latitude', 'longitude']).values
    except Exception as e:
        print(e)
    nmme_values[np.isnan(nmme_values)] = -9999
    return (nmme_values).tolist(), LTA


def get_date_range_from_nc_file(nc_file):
    start_date = nc_file["time"].values.min()
    t = pd.to_datetime(str(start_date))
    start_date = t.strftime('%Y-%m-%d')
    ed = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=180)
    end_date = ed.strftime('%Y-%m-%d')
    return start_date, end_date


def get_monthlyanalysis_dates_bounds(geom):
    # db.connections.close_all()
    # params = Parameters.objects.first()
    # Get start date and end date for NMME from netCDf file
    nc_file = xr.open_dataset(params.nmme_ccsm4_path + 'nmme-ccsm4_bcsd.latest.global.0.5deg.daily.ens001.nc4',
                              chunks={'time': 16, 'longitude': 128, 'latitude': 128})
    start_date, end_date = get_date_range_from_nc_file(nc_file)

    month_list = [i.strftime("%Y-%m-%d") for i in pd.date_range(start=start_date, end=end_date, freq='MS')]
    month_nums = [int(i.strftime("%m")) for i in pd.date_range(start=start_date, end=end_date, freq='MS')]

    # add properties to geometry json if it did not exist
    jsonn = json.loads(str(geom))
    for x in range(len(jsonn["features"])):
        if "properties" not in jsonn["features"][x]:
            jsonn["features"][x]["properties"] = {}
    json_aoi = json.dumps(jsonn)
    # using geopandas to get the bounds
    aoi = gpd.read_file(json_aoi)
    return month_list, month_nums, aoi.total_bounds


def write_to_tiff(data_object, uniqueid):
    # params = Parameters.objects.first()
    os.makedirs(params.zipFile_ScratchWorkspace_Path + uniqueid, exist_ok=True)
    os.chmod(params.zipFile_ScratchWorkspace_Path + uniqueid, 0o777)
    os.chdir(params.zipFile_ScratchWorkspace_Path + uniqueid)
    file_name = data_object.time.dt.strftime('%Y%m%d').values[0] + '.tif'
    try:
        data_object.load()
    except Exception as e:
        print(e)
    # print(fileName)
    width = data_object.longitude.size  # HOW DOES THIS CHANGE IF WE HAVE 2D LAT/LON ARRAYS
    height = data_object.latitude.size  # HOW DOES THIS CHANGE IF WE HAVE 2D LAT/LON ARRAYS
    data_type = str(data_object.dtype)
    missing_value = np.nan  # This could change if we are not using float arrays.
    crs = 'EPSG:4326'
    x_res = np.abs(
        data_object.longitude.values[1]
        - data_object.longitude.values[0])  # again, could change if using 2D coord arrays.
    y_res = np.abs(
        data_object.latitude.values[1]
        - data_object.latitude.values[0])  # again, could change if using 2D coord arrays.
    x_min = data_object.longitude.values.min() - x_res / 2.0  # shift to corner by 1/2 grid cell res
    y_max = data_object.latitude.values.max() + y_res / 2.0  # shift to corner by 1/2 grid cell res
    aff_transform = rio.transform.from_origin(x_min, y_max, x_res, y_res)
    # Open the file.
    dst = rio.open(file_name,
                   'w',
                   driver='GTiff',
                   dtype=data_type,
                   nodata=missing_value,
                   width=width,
                   height=height,
                   count=1,
                   crs=crs,
                   transform=aff_transform,
                   compress='lzw')
    # Write the data.
    try:
        dst.write(np.flip(data_object.values, axis=1))  # Note, we  flip the data along the latitude dimension
        # so that it is monotonically decreasing (i.e. N to S)
    except Exception as e:
        print(e)
        logger.info(e)

    # Close the file.
    dst.close()
    return file_name
