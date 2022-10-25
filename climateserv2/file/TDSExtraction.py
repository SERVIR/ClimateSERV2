import csv
import json
import os
from datetime import datetime, timedelta
from json import JSONDecodeError

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
import regionmask as rm
import xarray as xr

from api.models import ETL_Dataset
from api.models import Parameters
from django.db import IntegrityError, transaction
from api.models import Request_Progress
from frontend.models import DataLayer
from frontend.models import EnsembleLayer

try:
    import climateserv2.locallog.locallogging as llog

except ImportError:
    import locallog.locallogging as llog

logger = llog.getNamedLogger("request_processor")
params = Parameters.objects.first()


def get_filelist(datatype, start_date, end_date):
    try:
        if DataLayer.objects.filter(api_id=int(datatype)).exists():
            working_datalayer = DataLayer.objects.get(api_id=int(datatype))
            working_dataset = working_datalayer.etl_dataset_id
        else:
            working_datalayer = ETL_Dataset.objects.filter(number=int(datatype)).first()
            working_dataset = working_datalayer.etl_dataset_id
    except Exception as e:
        logger.info("failed to get dataset in get_filelist: " + str(e))
        print("failed to get dataset in get_filelist: " + str(e))
        raise e
    try:
        dataset_name_format = working_dataset.dataset_name_format
    except Exception as e:
        print("failed to get name format in get_filelist" + str(e))
        raise e
    dataset_nc4_variable_name = working_datalayer.layers
    year_nums = range(datetime.strptime(start_date, '%Y-%m-%d').year, datetime.strptime(end_date, '%Y-%m-%d').year + 1)
    filelist = []
    dataset_name = dataset_name_format.split('_')
    final_load_dir = working_dataset.fast_directory_path

    if not os.path.exists(final_load_dir):
        os.makedirs(final_load_dir)
    # All the info below should be able to come from the dataset db entry.
    # There should be no reason to hardcode any of this or have separate conditions.
    # In fact the fields exist in the current DataModel but are not being used ¯\_(ツ)_/¯
    # I stand corrected...
    # There should be a condition for yearly and monthly merge, which is not indicated in the
    # current DataModel, so we would have to hardcode a list of names for like.
    # if (str(dataset_name[0].lower()) in ['chirp', 'chirps_gefs', 'emodis']):
    #     ...add the month to the name, else don't.
    # nmme looks like it's different as well, so maybe 3 conditions

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
    elif "sport-lis" in dataset_name:
        for year in year_nums:
            name = final_load_dir + dataset_name[0] \
                   + ".africa." + dataset_name[2] + ".daily." + str(year) + ".nc4"
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


def update_progress(job_variables):
    job_length = job_variables["progress"]
    uniqueid = job_variables["uniqueid"]
    try:
        with transaction.atomic():
            request_progress = Request_Progress.objects.get(request_id=uniqueid)
            logger.debug("got request object for: " + uniqueid)
            logger.debug(str(job_length) + ' - was the job_length')
            update_value = (float(request_progress.progress) + ((100 / job_length) / 3)) - .5
            logger.debug(str(update_value) + '% done')
            request_progress.progress = update_value
            logger.debug("updated progress for: " + uniqueid)
            request_progress.save()
            logger.debug(str(uniqueid) + "***************************** " + str(request_progress.progress))
    except IntegrityError:
        logger.error("Progress update issue")


# To get the dates and values corresponding to the dataset, variable, dates, operation and geometry
def get_data_values(uniqueid, start_date, end_date, variable, geom, operation, file_list, job_length):
    logger.debug("Just entered get_data_values: " + uniqueid)
    update_progress({'progress': job_length, 'uniqueid': uniqueid})

    # Convert dates to %Y-%m-%d format for NetCDF
    try:
        st = datetime.strptime(start_date, '%m/%d/%Y')
        et = datetime.strptime(end_date, '%m/%d/%Y')
        start_date = datetime.strftime(st, '%Y-%m-%d')
        end_date = datetime.strftime(et, '%Y-%m-%d')
    except (ValueError, TypeError):
        # If there is an exception with date format while converting, we can just ignore the conversion and use the
        # passed dates
        pass
    logger.debug("past datetime stuff for: " + uniqueid)
    try:
        jsonn = json.loads(str(geom))
    except JSONDecodeError:
        jsonn = json.loads(json.dumps(geom))
    logger.debug("past json loads  for: " + uniqueid)
    for x in range(len(jsonn["features"])):
        if "properties" not in jsonn["features"][x]:
            jsonn["features"][x]["properties"] = {}

    # If the geometry is not a point, map the area of interest to the netCDF and extract values
    # If the geometry is a point, get dates and values from the openDAP URL as shown below (line 120)
    json_aoi = json.dumps(jsonn)
    geo_data_frame = gpd.read_file(json_aoi)
    logger.debug("past reading it for: " + uniqueid)
    lon1, lat1, lon2, lat2 = geo_data_frame.total_bounds
    # using xarray to open the temporary netcdf
    logger.debug("about to xarray open the data for: " + uniqueid)
    try:
        with xr.open_mfdataset(file_list,
                               parallel=True,
                               chunks={'time': 32, 'longitude': 500, 'latitude': 500},
                               autoclose=True) as nc_file:
            lat_slice, lon_slice = get_bounds_from_dataset(nc_file, lat1, lat2, lon1, lon2)

            unmasked_data = nc_file[variable].sel(longitude=lon_slice, latitude=lat_slice).sel(
                time=slice(start_date, end_date))
            nc_file.close()

        # nc_file = xr.open_mfdataset(file_list,
        #                             parallel=True,
        #                             chunks={'time': 32, 'longitude': 500, 'latitude': 500},
        #                             autoclose=True)
    except Exception as e:
        logger.error("open_mfdataset error: " + str(e) + " for: " + uniqueid)
        return [], []

    # lat_slice, lon_slice = get_bounds_from_dataset(nc_file, lat1, lat2, lon1, lon2)
    #
    # unmasked_data = nc_file[variable].sel(longitude=lon_slice, latitude=lat_slice).sel(time=slice(start_date, end_date))
    # nc_file.close()
    update_progress({'progress': job_length, 'uniqueid': uniqueid})
    if jsonn['features'][0]['geometry']['type'] == "Point":
        data = unmasked_data
    else:
        bool_mask = None
        try:
            aoi_combined = geo_data_frame.assign(combine=1).dissolve(by='combine', aggfunc='sum')
            bool_mask = rm.mask_3D_geopandas(aoi_combined, unmasked_data, lon_name='longitude',
                                             lat_name='latitude').squeeze(dim='region', drop=True)

        except Exception as mask_exception:
            logger.error("mask_exception: " + str(mask_exception))
            bool_mask = None
        if bool_mask is None:
            data = unmasked_data
        else:
            data = unmasked_data.where(bool_mask)

    dates = data.time.dt.strftime("%Y-%m-%d").values.tolist()
    logger.debug('operation: ' + operation)
    update_progress({'progress': job_length, 'uniqueid': uniqueid})
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
    elif operation == "netcdf":
        logger.debug("*********************NetCDF*******************************")
        try:
            os.makedirs(params.zipFile_ScratchWorkspace_Path + uniqueid, exist_ok=True)
            os.chmod(params.zipFile_ScratchWorkspace_Path + uniqueid, 0o777)
            data.to_netcdf(params.zipFile_ScratchWorkspace_Path + uniqueid + '/' + start_date + '-' + end_date + '.nc')
        except Exception as e:
            logger.error("to_netcdf or zip error: " + str(e))
    elif operation == "download" or operation == "csv":
        logger.debug("*********************download*******************************")
        if jsonn['features'][0]['geometry']['type'] == "Point":
            logger.debug("*********************download-Point*******************************")
            values = data.values
            values[np.isnan(values)] = -9999
            key_list = ["Date", "Value"]
            dct = {}
            for ind in range(len(dates)):
                dct[dates[ind]] = values[ind]
            with open(params.zipFile_ScratchWorkspace_Path + uniqueid + '.csv', "w") as file:
                outfile = csv.DictWriter(file, fieldnames=key_list)
                outfile.writeheader()
                if len(dates) > 0:
                    for k, v in dct.items():
                        outfile.writerow({"Date": k, "Value": v[0][0]})
                else:
                    outfile.writerow({"Date": "No data", "Value": "No data"})

            zip_file_path = params.zipFile_ScratchWorkspace_Path + uniqueid + '.csv'
        else:
            files = [write_to_tiff(data.sel(time=[x]), uniqueid) for x in data.time.values]
            if len(files) > 0:
                zip_file_path = params.zipFile_ScratchWorkspace_Path + uniqueid + '.zip'
            else:
                zip_file_path = ""
        return zip_file_path


# To retrive the CHIRPS data from 1981  to 2020 for Monthly Analysis.
# Retrieves 25th, 50th, 75th percentiles corresponding to month list from NMME
def get_chirps_climatology(month_nums, total_bounds, uniqueid):
    update_progress({'progress': 2, 'uniqueid': uniqueid})
    basepath = '/mnt/climateserv/process_tmp/downloads/chirps/ucsb-chirps-monthly-resolved-for-climatology.nc4'
    ds = xr.open_dataset(basepath, chunks={'time': 12, 'longitude': 128, 'latitude': 128})
    lon1, lat1, lon2, lat2 = total_bounds
    lat_slice, lon_slice = get_bounds_from_dataset(ds, lat1, lat2, lon1, lon2)
    precip = ds.precipitation_amount.sel(longitude=lon_slice, latitude=lat_slice).mean(dim=['latitude', 'longitude'])
    update_progress({'progress': 2, 'uniqueid': uniqueid})
    lta = precip.groupby('time.month').mean(
        dim='time').values  # this give the mean climatology for the spatial averages
    lta[np.isnan(lta)] = -9999
    value = precip.chunk(dict(time=-1)).groupby('time.month').quantile(q=[0.25, 0.50, 0.75], dim='time').values
    value[np.isnan(value)] = -9999
    res_arr = [list(value[month_nums[0] - 1]), list(value[month_nums[1] - 1]), list(value[month_nums[2] - 1]),
               list(value[month_nums[3] - 1]),
               list(value[month_nums[4] - 1]), list(value[month_nums[5] - 1])]
    lta_arr = [lta[month_nums[0] - 1], lta[month_nums[1] - 1], lta[month_nums[2] - 1], lta[month_nums[3] - 1],
               lta[month_nums[4] - 1], lta[month_nums[5] - 1]]
    update_progress({'progress': 2, 'uniqueid': uniqueid})
    return res_arr, lta_arr


def get_bounds_from_dataset(ds, lat1, lat2, lon1, lon2):
    lat_bounds = ds.sel(latitude=[lat1, lat2], method='nearest').latitude.values
    lon_bounds = ds.sel(longitude=[lon1, lon2], method='nearest').longitude.values
    lat_slice = slice(lat_bounds[0], lat_bounds[1])
    lon_slice = slice(lon_bounds[0], lon_bounds[1])
    return lat_slice, lon_slice


# To retrieve NMME data from start date of the netCDF to 180 days from start date
# Requires bounds of the geometry to get the data from CCSM4 and CFSV2 sensor files.
# Uses first five ensembles from both sensors
def get_nmme_data(total_bounds, uniqueid):
    # set number of ensembles to use from each dataset.
    update_progress({'progress': 2, 'uniqueid': uniqueid})
    num_ens = 5
    lta = []
    for ens in np.arange(num_ens):
        lon1, lat1, lon2, lat2 = total_bounds
    base_path = '/mnt/climateserv/process_tmp/fast_nmme_monthly/nmme-mme_bcsd.latest.global.0.5deg.daily.nc4'
    ds = xr.open_dataset(base_path, chunks={'time': 7, 'longitude': 256, 'latitude': 256})
    update_progress({'progress': 2, 'uniqueid': uniqueid})
    lat_slice, lon_slice = get_bounds_from_dataset(ds, lat1, lat2, lon1, lon2)
    try:
        nmme_values = ds.precipitation.sel(longitude=lon_slice, latitude=lat_slice).mean(
            dim=['latitude', 'longitude']).values
    except Exception as e:
        logger.error("get_nmme_data Error: " + str(e))
        nmme_values = []
    nmme_values[np.isnan(nmme_values)] = -9999
    update_progress({'progress': 2, 'uniqueid': uniqueid})
    return nmme_values.tolist(), lta


def get_date_range_from_nc_file(nc_file):
    start_date = nc_file["time"].values.min()
    t = pd.to_datetime(str(start_date))
    start_date = t.strftime('%Y-%m-%d')
    ed = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=180)
    end_date = ed.strftime('%Y-%m-%d')
    return start_date, end_date


def get_monthly_analysis_dates_bounds(geom):
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
