'''
Created on Jun 3, 2014
@author: jeburks

Modified starting from Sept 2015
@author: Kris Stanton
'''

from .processtools import dateIndexTools as dit

DEBUG_LIVE = False

logToConsole = True
serviringestroot = '''/data/data/cserv/pythonCode/serviringest/'''
maskstorage = '''D:/tmp/'''# '''/data/data/mask/'''
dbfilepath = '''D:/tmp/servirchirps.db'''#'''/data/data/db/servirchirps.db'''
newdbfilepath = '''D:/tmp/servirchirps_bsddb.db''' #newdbfilepath = '''/data/data/db/servirchirps_bsddb.db'''
capabilities_db_filepath = '''D:/tmp/servirchirps_bsddb_capabilities.db''' #/data/data/db/servirchirps_bsd_capabilities.db'''
requestLog_db_basepath = '''D:/tmp/''' #'''/data/data/db/requestLog/'''
zipFile_MediumTermStorage_Path = '''/data/data/zipout/Zipfile_Storage/'''  # TODO!! ADD AUTO FOLDER CREATION FOR THIS FOLDER
zipFile_ScratchWorkspace_Path = '''/data/data/zipout/Zipfile_Scratch/'''  # TODO!! ADD AUTO FOLDER CREATION FOR THIS FOLDER
# zipFile_MediumTermStorage_Path = '''/data2/data2/zipout/Zipfile_Storage/'''  # TODO!! ADD AUTO FOLDER CREATION FOR THIS FOLDER
# zipFile_ScratchWorkspace_Path = '''/data2/data2/zipout/Zipfile_Scratch/'''  # TODO!! ADD AUTO FOLDER CREATION FOR THIS FOLDER
logfilepath = '''D:/tmp/'''#'''/data/data/logs/'''
workpath = '''D:/tmp/'''#'''/data/data/work'''
shapefilepath = '''D:/tmp/mapfiles/''' #'''/data/data/gis/mapfiles/'''
ageInDaysToPurgeData = 7

# parameters = ([0,'max',"Max"],[1,'min',"Min"],[2,'median',"Median"],[3,'range',"Range"],[4,'sum',"Sum"],[5,'avg','Average'])
parameters = [[0, 'max', "Max"], [1, 'min', "Min"], [2, 'median', "Median"], [3, 'range', "Range"], [4, 'sum', "Sum"],
              [5, 'avg', 'Average'], [6, 'download', 'Download']]


# KS Refactor // Adding Climate Change Model datatype param getter functions
def get_ClimateChangeParam__directory(ensembleName, variableName):
    return '/data/data/image/processed/nmme/' + str(ensembleName) + '/' + str(variableName) + '/'


def get_ClimateChangeParam__inputDataLocation(ensembleName, variableName):
    return '/data/data/image/input/nmme/' + str(ensembleName) + '/' + str(variableName) + '/'


def get_ClimateChangeParam__modelOutputLocal_BaseFolder():
    return '/data/data/cserv/nmmeInputTemp/'  # return '/data/nmme/GEOOUT/'


def get_ClimateChangeParam__size():
    return [720, 360]


def get_ClimateChangeParam__fillValue():
    return -9999.


def get_ClimateChangeParam__indexer():
    return dit.DailyIndex()


def get_ClimateChangeParam__number_Of_ForecastDays():
    return 180


dataTypes = [
    {'number': 0,
     'name': 'Chirps', 'description': 'Global CHIRPS Dataset ',
     'size': [7200, 2000],
     'directory': '/data/data/image/processed/chirps/',
     'fillValue': -9999.,
     'indexer': dit.DailyIndex(),
     'inputDataLocation': '/data/data/image/input/chirps/global/',
     'data_category': 'CHIRPS'
     },

    {'number': 1,
     'name': 'NDVI MODIS-West Africa',
     'description': 'NDVI MODIS West Africa description',
     'size': [19272, 7875],
     'directory': '/data/data/image/processed/eMODIS/ndvi-westafrica/',
     'fillValue': -9999.,
     'indexer': dit.DecadalIndex(),
     'inputDataLocation': '/data/data/image/input/emodis/westafrica/',
     'data_category': 'NDVI'
     },

    {'number': 2,
     'name': 'NDVI MODIS-East Africa',
     'description': 'NDVI MODIS East Africa description',
     'size': [12847, 14713],
     'directory': '/data/data2/image/processed/eMODIS/ndvi-eastafrica/np/',
     'fillValue': -9999.,
     'indexer': dit.DecadalIndex(),
     'inputDataLocation': '/data/data2/image/input/emodis/eastafrica/',
     'data_category': 'NDVI'
     },

    {'number': 3,
     'name': 'NDVI MODIS-North Africa', 'description': 'NDVI MODIS North Africa description',
     'size': [23415, 7045],
     'directory': '/data/data/image/processed/eMODIS/ndvi-northafrica/np/',
     'fillValue': -9999.,
     'indexer': dit.EveryFiveDaysIndex(),
     'inputDataLocation': '/data/data/image/input/emodis/northafrica/',
     'data_category': 'NDVI'
     },

    {'number': 4,
     'name': 'NDVI MODIS-Central Africa',
     'description': 'NDVI MODIS Central Africa description',
     'size': [1600, 1500],
     'directory': '/data/data/image/processed/eMODIS/ndvi-centralafrica/np/',
     'fillValue': -9999.,
     'indexer': dit.EveryFiveDaysIndex(),
     'inputDataLocation': '/data/data/image/input/emodis/centralafrica/',
     'data_category': 'NDVI'
     },

    {'number': 5, 'name': 'NDVI MODIS-South Africa',
     'description': 'NDVI MODIS South Africa description',
     'size': [19839, 17001],
     'directory': '/data/data2/image/processed/eMODIS/ndvi-southafrica/np/',
     'fillValue': -9999.,
     'indexer': dit.DecadalIndex(),
     'inputDataLocation': '/data/data2/image/input/emodis/southafrica/',
     'data_category': 'NDVI'
     },

    # KS Note: Adding new climate change datatypes here.
    # Also added a few new properties to these.  See 'datatypes_Doc.txt' for more detailed info about these changes

    # Ensemble 01
    {
        'number': 6,
        'data_category': 'ClimateModel',
        'ensemble': 'ens01', 'variable': 'tref',
        'ensemble_Label': 'Ensemble 01', 'variable_Label': 'Temperature',
        'name': 'Climate Change Scenario: ens01 Temperature',
        'description': 'Climate Change Scenario: ens01 Temperature',
        'directory': get_ClimateChangeParam__directory('ens01', 'tref'),
        # '/Users/kris/work/SERVIR/Data/Processed/climatechange/ens01/temperature/',
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens01', 'tref'),
        # '/Users/kris/work/SERVIR/Data/Image/climatechange/ens01/temperature/',
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        # '/Users/kris/work/temp_NMME_Data/server_fs/data/nmme/GEOOUT/',
        'size': get_ClimateChangeParam__size(),  # [720,360]
        'fillValue': get_ClimateChangeParam__fillValue(),  # -9999.,
        'indexer': get_ClimateChangeParam__indexer(),  # dit.DailyIndex(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()  # 180,
    },

    {
        'number': 7,
        'data_category': 'ClimateModel',
        'ensemble': 'ens01', 'variable': 'prcp',
        'ensemble_Label': 'Ensemble 01', 'variable_Label': 'Precipitation',
        'name': 'Climate Change Scenario: ens01 Precipitation',
        'description': 'Climate Change Scenario: ens01 Precipitation',
        'directory': get_ClimateChangeParam__directory('ens01', 'prcp'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens01', 'prcp'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    # Ensemble 02
    {
        'number': 8,
        'data_category': 'ClimateModel',
        'ensemble': 'ens02', 'variable': 'tref',
        'ensemble_Label': 'ensemble 02', 'variable_Label': 'Temperature',
        'name': 'Climate Change Scenario: ens02 Temperature',
        'description': 'Climate Change Scenario: ens02 Temperature',
        'directory': get_ClimateChangeParam__directory('ens02', 'tref'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens02', 'tref'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    {
        'number': 9,
        'data_category': 'ClimateModel',
        'ensemble': 'ens02', 'variable': 'prcp',
        'ensemble_Label': 'Ensemble 02', 'variable_Label': 'Precipitation',
        'name': 'Climate Change Scenario: ens02 Precipitation',
        'description': 'Climate Change Scenario: ens02 Precipitation',
        'directory': get_ClimateChangeParam__directory('ens02', 'prcp'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens02', 'prcp'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    # Ensemble 03
    {
        'number': 10,
        'data_category': 'ClimateModel',
        'ensemble': 'ens03', 'variable': 'tref',
        'ensemble_Label': 'ensemble 03', 'variable_Label': 'Temperature',
        'name': 'Climate Change Scenario: ens03 Temperature',
        'description': 'Climate Change Scenario: ens03 Temperature',
        'directory': get_ClimateChangeParam__directory('ens03', 'tref'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens03', 'tref'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    {
        'number': 11,
        'data_category': 'ClimateModel',
        'ensemble': 'ens03', 'variable': 'prcp',
        'ensemble_Label': 'Ensemble 03', 'variable_Label': 'Precipitation',
        'name': 'Climate Change Scenario: ens03 Precipitation',
        'description': 'Climate Change Scenario: ens03 Variable: Precipitation',
        'directory': get_ClimateChangeParam__directory('ens03', 'prcp'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens03', 'prcp'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    # Ensemble 04
    {
        'number': 12,
        'data_category': 'ClimateModel',
        'ensemble': 'ens04', 'variable': 'tref',
        'ensemble_Label': 'ensemble 04', 'variable_Label': 'Temperature',
        'name': 'Climate Change Scenario: ens04 Temperature',
        'description': 'Climate Change Scenario: ens04 Temperature',
        'directory': get_ClimateChangeParam__directory('ens04', 'tref'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens04', 'tref'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    {
        'number': 13,
        'data_category': 'ClimateModel',
        'ensemble': 'ens04', 'variable': 'prcp',
        'ensemble_Label': 'Ensemble 04', 'variable_Label': 'Precipitation',
        'name': 'Climate Change Scenario: ens04 Precipitation',
        'description': 'Climate Change Scenario: ens04 Precipitation',
        'directory': get_ClimateChangeParam__directory('ens04', 'prcp'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens04', 'prcp'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    # Ensemble 05
    {
        'number': 14,
        'data_category': 'ClimateModel',
        'ensemble': 'ens05', 'variable': 'tref',
        'ensemble_Label': 'ensemble 05', 'variable_Label': 'Temperature',
        'name': 'Climate Change Scenario: ens05 Temperature',
        'description': 'Climate Change Scenario: ens05 Temperature',
        'directory': get_ClimateChangeParam__directory('ens05', 'tref'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens05', 'tref'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    {
        'number': 15,
        'data_category': 'ClimateModel',
        'ensemble': 'ens05', 'variable': 'prcp',
        'ensemble_Label': 'Ensemble 05', 'variable_Label': 'Precipitation',
        'name': 'Climate Change Scenario: ens05 Precipitation',
        'description': 'Climate Change Scenario: ens05 Precipitation',
        'directory': get_ClimateChangeParam__directory('ens05', 'prcp'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens05', 'prcp'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    # Ensemble 06
    {
        'number': 16,
        'data_category': 'ClimateModel',
        'ensemble': 'ens06', 'variable': 'tref',
        'ensemble_Label': 'ensemble 06', 'variable_Label': 'Temperature',
        'name': 'Climate Change Scenario: ens06 Temperature',
        'description': 'Climate Change Scenario: ens06 Temperature',
        'directory': get_ClimateChangeParam__directory('ens06', 'tref'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens06', 'tref'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    {
        'number': 17,
        'data_category': 'ClimateModel',
        'ensemble': 'ens06', 'variable': 'prcp',
        'ensemble_Label': 'Ensemble 06', 'variable_Label': 'Precipitation',
        'name': 'Climate Change Scenario: ens06 Precipitation',
        'description': 'Climate Change Scenario: ens06 Precipitation',
        'directory': get_ClimateChangeParam__directory('ens06', 'prcp'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens06', 'prcp'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    # Ensemble 07
    {
        'number': 18,
        'data_category': 'ClimateModel',
        'ensemble': 'ens07', 'variable': 'tref',
        'ensemble_Label': 'ensemble 07', 'variable_Label': 'Temperature',
        'name': 'Climate Change Scenario: ens07 Temperature',
        'description': 'Climate Change Scenario: ens07 Temperature',
        'directory': get_ClimateChangeParam__directory('ens07', 'tref'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens07', 'tref'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    {
        'number': 19,
        'data_category': 'ClimateModel',
        'ensemble': 'ens07', 'variable': 'prcp',
        'ensemble_Label': 'Ensemble 07', 'variable_Label': 'Precipitation',
        'name': 'Climate Change Scenario: ens07 Precipitation',
        'description': 'Climate Change Scenario: ens07 Precipitation',
        'directory': get_ClimateChangeParam__directory('ens07', 'prcp'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens07', 'prcp'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    # Ensemble 08
    {
        'number': 20,
        'data_category': 'ClimateModel',
        'ensemble': 'ens08', 'variable': 'tref',
        'ensemble_Label': 'ensemble 08', 'variable_Label': 'Temperature',
        'name': 'Climate Change Scenario: ens08 Temperature',
        'description': 'Climate Change Scenario: ens08 Temperature',
        'directory': get_ClimateChangeParam__directory('ens08', 'tref'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens08', 'tref'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    {
        'number': 21,
        'data_category': 'ClimateModel',
        'ensemble': 'ens08', 'variable': 'prcp',
        'ensemble_Label': 'Ensemble 08', 'variable_Label': 'Precipitation',
        'name': 'Climate Change Scenario: ens08 Precipitation',
        'description': 'Climate Change Scenario: ens08 Precipitation',
        'directory': get_ClimateChangeParam__directory('ens08', 'prcp'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens08', 'prcp'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    # Ensemble 09
    {
        'number': 22,
        'data_category': 'ClimateModel',
        'ensemble': 'ens09', 'variable': 'tref',
        'ensemble_Label': 'ensemble 09', 'variable_Label': 'Temperature',
        'name': 'Climate Change Scenario: ens09 Temperature',
        'description': 'Climate Change Scenario: ens09 Temperature',
        'directory': get_ClimateChangeParam__directory('ens09', 'tref'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens09', 'tref'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    {
        'number': 23,
        'data_category': 'ClimateModel',
        'ensemble': 'ens09', 'variable': 'prcp',
        'ensemble_Label': 'Ensemble 09', 'variable_Label': 'Precipitation',
        'name': 'Climate Change Scenario: ens09 Precipitation',
        'description': 'Climate Change Scenario: ens09 Precipitation',
        'directory': get_ClimateChangeParam__directory('ens09', 'prcp'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens09', 'prcp'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    # Ensemble 10
    {
        'number': 24,
        'data_category': 'ClimateModel',
        'ensemble': 'ens10', 'variable': 'tref',
        'ensemble_Label': 'ensemble 10', 'variable_Label': 'Temperature',
        'name': 'Climate Change Scenario: ens10 Temperature',
        'description': 'Climate Change Scenario: ens10 Temperature',
        'directory': get_ClimateChangeParam__directory('ens10', 'tref'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens10', 'tref'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    {
        'number': 25,
        'data_category': 'ClimateModel',
        'ensemble': 'ens10', 'variable': 'prcp',
        'ensemble_Label': 'Ensemble 10', 'variable_Label': 'Precipitation',
        'name': 'Climate Change Scenario: ens10 Precipitation',
        'description': 'Climate Change Scenario: ens10 Precipitation',
        'directory': get_ClimateChangeParam__directory('ens10', 'prcp'),
        'inputDataLocation': get_ClimateChangeParam__inputDataLocation('ens10', 'prcp'),
        'modelOutputLocal_BaseFolder': get_ClimateChangeParam__modelOutputLocal_BaseFolder(),
        'size': get_ClimateChangeParam__size(), 'fillValue': get_ClimateChangeParam__fillValue(),
        'indexer': get_ClimateChangeParam__indexer(),
        'number_Of_ForecastDays': get_ClimateChangeParam__number_Of_ForecastDays()
    },

    # End First batch of ClimateModel datasets

    # IMERG Precip, GPM
    {'number': 26,
     'name': 'IMERG 1 Day',
     'description': 'GPM IMERG Daily Accumulation Precip',
     'size': [3600, 1800],
     'directory': '/data/data2/image/processed/IMERG1Day/',
     'fillValue': 9999.,
     'indexer': dit.DynamicIndex("P1D"),
     'inputDataLocation': '/data/data2/image/input/IMERG1Day/',
     'data_category': 'IMERG'
     },
    {'number': 27, 'name': 'NDVI MODIS-Afghanistan',
     'description': 'NDVI MODIS Afghanistan description',
     'size': [6631, 4144],
     'directory': '/data/data2/image/processed/eMODIS/ndvi-afghanistan/np/',
     'fillValue': -9999.,
     'indexer': dit.EveryFiveDaysIndex(),
     'inputDataLocation': '/data/data2/image/input/emodis/afghanistan/',
     'data_category': 'NDVI'
     },
    {'number': 28, 'name': 'NDVI MODIS-Asia',
     'description': 'NDVI MODIS Asia description',
     'size': [17407, 13676],
     'directory': '/data/data2/image/processed/eMODIS/ndvi-asia/np/',
     'fillValue': -9999.,
     'indexer': dit.DecadalIndex(),
     'inputDataLocation': '/data/data2/image/input/emodis/asia/',
     'data_category': 'NDVI'
     },
    {'number': 29,
     'name': 'ESI', 'description': 'Global ESI  Dataset ',
     'size': [7200, 3000],
     'directory': '/data/data3/image/processed/esi/4WK/',
     'fillValue': -9999.,
     'indexer': dit.DynamicIndex("P8D"),  # dit.EveryEightDaysIndex(),
     'inputDataLocation': '/data/data/image/input/esi/4WK/',
     'data_category': 'ESI'
     },
    {'number': 30,
     'name': 'SMAP', 'description': 'SMAP  Dataset ',
     'size': [964, 406],
     'directory': '/data/data2/image/processed/SMAP/',
     'fillValue': -9999.,
     'indexer': dit.DailyIndex(),
     'inputDataLocation': '/data/data2/image/input/smap/',
     'data_category': 'SMAP'
     },
    {'number': 31,
     'name': 'Chirps-GEFS-Anom', 'description': 'Global CHIRPS GEFS Anomalies Dataset ',
     'size': [7200, 2000],
     'directory': '/data/data3/image/processed/gefs_anom/',
     'fillValue': -9999.,
     'indexer': dit.DecadalIndex(),
     'inputDataLocation': '/data/data3/image/input/gefs_anom/',
     'data_category': 'CHIRPS'
     },
    {'number': 32,
     'name': 'Chirps-GEFS-Precip', 'description': 'Global CHIRPS GEFS Precipitation Dataset ',
     'size': [7200, 2000],
     'directory': '/data/data3/image/processed/gefs_precip/',
     'fillValue': -9999.,
     'indexer': dit.DailyIndex(),  # dit.DecadalIndex(),
     'inputDataLocation': '/data/data3/image/input/gefs_precip/',
     'data_category': 'CHIRPS'
     },
    # There will be more added here in time.
    {'number': 33,
     'name': 'ESI', 'description': 'Global ESI  Dataset ',
     'size': [7200, 3000],
     'directory': '/data/data3/image/processed/esi/12WK/',
     'fillValue': -9999.,
     'indexer': dit.DynamicIndex("P8D"),  # dit.EveryEightDaysIndex(),
     'inputDataLocation': '/data/data/image/input/esi/12WK/',
     'data_category': 'ESI'
     },
    {'number': 34,
     'name': 'IMERG Test',
     'description': 'GPM IMERG Daily Accumulation Precip',
     'size': [3600, 1800],
     'directory': '/data/data2/image/processed/IMERGTest/',
     'fillValue': 9999.,
     'indexer': dit.DynamicIndex("P1D"),
     'inputDataLocation': '/data/data2/image/input/IMERGTest/',
     'data_category': 'IMERG'
     },
    {'number': 35,
     'name': 'Chirps-GEFS-Precip', 'description': 'Global CHIRPS GEFS 25th Percentile Dataset ',
     'size': [7200, 2000],
     'directory': '/data/data3/image/processed/gefs_precip_25/',
     'fillValue': -9999.,
     'indexer': dit.DailyIndex(),  # dit.DecadalIndex(),
     'inputDataLocation': '/data/data3/image/input/gefs_precip_25/',
     'data_category': 'CHIRPS'
     },
    {'number': 36,
     'name': 'Chirps-GEFS-Precip', 'description': 'Global CHIRPS GEFS 75th Percentile Dataset ',
     'size': [7200, 2000],
     'directory': '/data/data3/image/processed/gefs_precip_75/',
     'fillValue': -9999.,
     'indexer': dit.DailyIndex(),  # dit.DecadalIndex(),
     'inputDataLocation': '/data/data3/image/input/gefs_precip_75/',
     'data_category': 'CHIRPS'
     },

    # There will be more added here in time.

]
dataTypeInfo = [{'path': '/data/data/gis/shapefiles', 'layer': 'county'}]
shapefileName = [{'id': 'country',
                  'displayName': "Countries",
                  'shapefilename': 'country.shp',
                  'visible': 'true'},
                 {'id': 'admin_1_earth',
                  'displayName': "Admin #1",
                  'shapefilename': 'admin_1_earth.shp',
                  'visible': 'false'},
                 {'id': 'admin_2_af',
                  'displayName': "Admin #2",
                  'shapefilename': 'admin_2_af.shp',
                  'visible': 'false'},
                 {'id': 'sept2015_zoi',
                  'displayName': "FTF ZOI",
                  'shapefilename': 'sept2015_zoi.shp',
                  'visible': 'false'}
                 ]
operationTypes = [
    [0, 'max', "Max"],
    [1, 'min', "Min"],
    [2, 'median', "Median"],
    [3, 'range', "Range"],
    [4, 'sum', "Sum"],
    [5, 'avg', "Average"],
    [6, 'getraw', "GetRawData"]  # ks refactor 2015 // New Operation : Get Raw Data and return it to the user.
]
intervals = [
    {'name': 'day', 'pattern': '%m/%d/%Y'},
    {'name': 'month', 'pattern': '%m/%Y'},
    {'name': 'year', 'pattern': '%Y'}
]

resultsdir = '''/data/data/results/'''

