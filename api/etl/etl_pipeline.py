import glob, os, sys

from api.services import Config_SettingService, ETL_DatasetService, ETL_GranuleService, ETL_LogService, ETL_PipelineRunService

from ..models import Config_Setting, ETL_Dataset

from ..serializers import ETL_DatasetSerializer

from .etl_dataset_subtype_chirps import ETL_Dataset_Subtype_CHIRPS
from .etl_dataset_subtype_chirps_gefs import ETL_Dataset_Subtype_CHIRPS_GEFS
from .etl_dataset_subtype_emodis import ETL_Dataset_Subtype_EMODIS
from .etl_dataset_subtype_esi import ETL_Dataset_Subtype_ESI
from .etl_dataset_subtype_imerg import ETL_Dataset_Subtype_IMERG
from .etl_dataset_subtype_nmme import ETL_Dataset_Subtype_NMME
from .etl_dataset_subtype_nmme_cfsv2 import ETL_Dataset_Subtype_NMME_CFSV2
from .etl_dataset_subtype_imerg import ETL_Dataset_Subtype_IMERG
from .etl_dataset_subtype_usda_smap import ETL_Dataset_Subtype_USDA_SMAP
from .etl_dataset_subtype_nsidc_smap_36km import ETL_Dataset_Subtype_NSIDC_SMAP_36KM
from .etl_dataset_subtype_nsidc_smap_9km import ETL_Dataset_Subtype_NSIDC_SMAP_9KM
from .etl_dataset_subtype_esi_servir import ETL_Dataset_Subtype_ESI_SERVIR

from . import etl_exceptions

class ETL_Pipeline():

    # Pipeline Run - UUID - What is the Database UUID for this ETL Pipeline Run Instance
    ETL_PipelineRun__UUID = ""  # Set when a new Database object is created.

    # Pipeline Config Params - Set Externally (only the etl_dataset_uuid param is actually required.  The rest are optional)
    etl_dataset_uuid            = ""
    no_duplicates               = True
    from_last_processed         = False
    merge_yearly                = False
    merge_monthly               = False
    START_YEAR_YYYY             = ""
    END_YEAR_YYYY               = ""
    START_MONTH_MM              = ""
    END_MONTH_MM                = ""
    START_DAY_DD                = ""
    END_DAY_DD                  = ""

    # Pipeline - Dataset Config Options - Set by Reading Dataset Item from the Database
    dataset_name = ""
    dataset_JSONable_Object = {}

    # Keeps a list of all the ETL Log IDs created during this run.
    new_etl_log_ids__EVENTS = []
    # Keeps a list of all the ETL Logs that are Errors created during this run.
    new_etl_log_ids__ERRORS = []
    # Keeps a list of all the new ETL Granules that were created during this run.
    new_etl_granule_ids = []
    # Keeps a list of all Granules that have Errors.
    new_etl_granule_ids__ERRORS = []
    # Keeps track of any affected Available Granule from this run (The nature of 'Available Granules' are Create_Or_Update, so this is just a list of affected ids.
    affected_Available_Granule_ids = []

    # A simple and quick way to tell if an error occurred in the pipeline.
    pipeline_had_error = False

    # This is the holder for the subtype instance
    Subtype_ETL_Instance = None

    # Default Constructor
    def __init__(self, etl_dataset_uuid):
        self.class_name = "ETL_Pipeline"

        self.etl_dataset_uuid = etl_dataset_uuid

    # Overriding the string function
    def __str__(self):
        outString = ""
        try:
            outString += "class_name: {}, etl_dataset_uuid: {}".format(str(self.class_name), str(self.etl_dataset_uuid))
        except:
            pass
        return outString

    # Function for quick output of all pipeline properties - Mainly used for easy debugging
    def to_JSONable_Object(self):
        retObj = {}

        # Pipeline Run UUID
        retObj["ETL_PipelineRun__UUID"]     = str(self.ETL_PipelineRun__UUID).strip()

        # Pipeline Config Options
        retObj["etl_dataset_uuid"]          = str(self.etl_dataset_uuid).strip()
        # Year Range
        retObj["START_YEAR_YYYY"]           = str(self.START_YEAR_YYYY).strip()
        retObj["END_YEAR_YYYY"]             = str(self.END_YEAR_YYYY).strip()
        # Month Range
        retObj["START_MONTH_MM"]    = str(self.START_MONTH_MM).strip()
        retObj["END_MONTH_MM"]      = str(self.END_MONTH_MM).strip()
        # Day Range
        retObj["START_DAY_DD"]      = str(self.START_DAY_DD).strip()
        retObj["END_DAY_DD"]        = str(self.END_DAY_DD).strip()

        # Pipeline - Dataset Config Options - Set by Reading From the Database
        retObj["dataset_name"]              = str(self.dataset_name).strip()
        retObj["dataset_JSONable_Object"]   = str(self.dataset_JSONable_Object).strip()

        retObj["new_etl_log_ids__EVENTS"]   = str(self.new_etl_log_ids__EVENTS).strip()
        retObj["new_etl_log_ids__ERRORS"]   = str(self.new_etl_log_ids__ERRORS).strip()

        retObj["new_etl_granule_ids"]               = str(self.new_etl_granule_ids).strip()
        retObj["new_etl_granule_ids__ERRORS"]       = str(self.new_etl_granule_ids__ERRORS).strip()
        retObj["affected_Available_Granule_ids"]    = str(self.affected_Available_Granule_ids).strip()

        return retObj

    # Standard UTIL functions (Checking for and Making Directories, parsing file names, handling datetime objects, etc)

    # Utility for creating a directory if one does not exist.
    def create_dir_if_not_exist(self, dir_path):
        ret_IsError = False
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
                activity_event_type     = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_DIRECTORY_CREATED", default_or_error_return_value="Directory Created")
                activity_description    = "New Directory created at path: " + str(dir_path)
                additional_json         = self.to_JSONable_Object()
                self.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
                ret_IsError = False
            except:
                # Log the Error (Unable to create a new directory)
                sysErrorData = str(sys.exc_info())
                activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
                activity_description = "Unable to create new directory at: " + str(dir_path) + ".  Sys Error Message: " + str(sysErrorData)
                additional_json = self.to_JSONable_Object()
                self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
                ret_IsError = True
        # END OF        if not os.path.exists(dir_path):
        return ret_IsError

    # ###########################################################################################
    # # STANDARD WRAPPER FUNCTIONS - Used by this class and many of the ETL Script Sub Classes
    # ###########################################################################################

    # Standard Function to record Events to the database
    # Wrapper for creating a row in the ETL Log Table (Logging Events) - Defaults are set
    def log_etl_event(self, activity_event_type="default_activity", activity_description="", etl_granule_uuid="", is_alert=False, additional_json={}):
        etl_log_row_uuid = ETL_LogService.create_etl_log_row(
            activity_event_type=activity_event_type,
            activity_description=activity_description,
            etl_pipeline_run_uuid=self.ETL_PipelineRun__UUID,
            etl_dataset_uuid=self.etl_dataset_uuid,
            etl_granule_uuid=etl_granule_uuid,
            is_alert=is_alert,
            created_by="ETL_PIPELINE__" + self.dataset_name,
            additional_json=additional_json
        )
        self.new_etl_log_ids__EVENTS.append(etl_log_row_uuid)

    # Standard Function to record Errors to the database
    # Wrapper for creating a row in the ETL Log Table but with Error info set and storing the Error ID Row
    def log_etl_error(self, activity_event_type="default_error", activity_description="an error occurred", etl_granule_uuid="", is_alert=True, additional_json={}):
        etl_log_row_uuid = ETL_LogService.create_etl_log_row(
            activity_event_type=activity_event_type,
            activity_description=activity_description,
            etl_pipeline_run_uuid=self.ETL_PipelineRun__UUID,
            etl_dataset_uuid=self.etl_dataset_uuid,
            etl_granule_uuid=etl_granule_uuid,
            is_alert=is_alert,
            created_by="ETL_PIPELINE__" + self.dataset_name,
            additional_json=additional_json
        )
        self.new_etl_log_ids__EVENTS.append(etl_log_row_uuid)
        self.new_etl_log_ids__ERRORS.append(etl_log_row_uuid)
        # Output something to the terminal - this can be very helpful during debugging.
        print('\n  ERROR: (error description): {}\n'.format(str(activity_description)))
        # Signal the pipeline object that errors did occur.
        self.pipeline_had_error = True

    # Standard Function to record all Attempted Granules for this pipeline run to the Database.
    # Wrapper for creating
    def log_etl_granule(self, granule_name="unknown_etl_granule_file_or_object_name", granule_contextual_information="", granule_pipeline_state="ATTEMPTING", additional_json={}):
        self__etl_pipeline_run_uuid     = self.ETL_PipelineRun__UUID
        self__etl_dataset_uuid          = self.etl_dataset_uuid
        self__etl_dataset_name          = "ETL_PIPELINE__" + self.dataset_name
        etl_Granule_Row_UUID = ETL_GranuleService.create_new_ETL_Granule_row(
            granule_name=granule_name,
            granule_contextual_information=granule_contextual_information,
            etl_pipeline_run_uuid=self__etl_pipeline_run_uuid,
            etl_dataset_uuid=self__etl_dataset_uuid,
            granule_pipeline_state=granule_pipeline_state,
            created_by=self__etl_dataset_name,
            additional_json=additional_json
        )
        self.new_etl_granule_ids.append(etl_Granule_Row_UUID)
        return etl_Granule_Row_UUID

    # Standard Function to update the State of an individual ETL Granule's granule_pipeline_state property - (When a granule has succeeded or failed)
    def etl_granule__Update__granule_pipeline_state(self, granule_uuid, new__granule_pipeline_state, is_error=False):
        is_update_succeed = ETL_GranuleService.update_existing_ETL_Granule__granule_pipeline_state(granule_uuid=granule_uuid, new__granule_pipeline_state=new__granule_pipeline_state)
        if is_error == True:
            self.new_etl_granule_ids__ERRORS.append(granule_uuid)
            # Placing this function call here means we don't have to ever call this from the type specific classes (Custom ETL Classes)
            is_update_succeed_2 = self.etl_granule__Update__is_missing_bool_val(granule_uuid=granule_uuid, new__is_missing__Bool_Val=True)
        return is_update_succeed

    # Standard Function to update the State of an individual if it is missing from the database or not. Updates the ETL Granule's is_missing property (bool)
    def etl_granule__Update__is_missing_bool_val(self, granule_uuid, new__is_missing__Bool_Val):
        is_update_succeed = ETL_GranuleService.update_existing_ETL_Granule__is_missing_bool_val(granule_uuid=granule_uuid, new__is_missing__Bool_Val=new__is_missing__Bool_Val)
        return is_update_succeed

    # Standard Function for adding new JSON data to an etl_granule (Expected Use Case: if we have an error, we can attach error info as a new JSON object to the existing record)
    def etl_granule__Append_JSON_To_Additional_JSON(self, granule_uuid, new_json_key_to_append, sub_jsonable_object):
        is_update_succeed = ETL_GranuleService.update_existing_ETL_Granule__Append_To_Additional_JSON(granule_uuid=granule_uuid, new_json_key_to_append=new_json_key_to_append, sub_jsonable_object=sub_jsonable_object)
        return is_update_succeed

    # Convenient function to call just before using a return statement during 'execute_pipeline_control_function'
    def log__pipeline_run__exit(self):
        # Log Activity - Pipeline Ended
        activity_event_type     = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_ENDED", default_or_error_return_value="ETL Pipeline Ended")
        activity_description    = "Pipeline Completed for Dataset: " + str(self.dataset_name)
        additional_json         = self.to_JSONable_Object()
        self.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
        #
        # Print some output for the console.
        if self.pipeline_had_error == True:
            print("")
            print("  AT LEAST ONE ERROR OCCURRED DURING THIS LAST PIPELINE RUN.")
            print("    Open up the Admin tool to view the error alerts, or open the Django Admin in order to see them directly.")
            print("    For quick reference, below you can find the output of the pipeline state from the last run.")
            print("")
            print("================ START ---- TERMINAL DEBUG INFO ---- START ================")
            print(str(self.to_JSONable_Object()))
            print("================= END ----- TERMINAL DEBUG INFO ----- END =================")
            print("")

    # Execute The Pipeline based on what ever was pre-configured.
    # # This is the main control function for the ETL pipeline
    def execute_pipeline_control_function(self):

        try:

            list_of_valid__dataset_subtypes = ETL_DatasetService.get_all_subtypes_as_string_array()

            # Create a new ETL_PipelineRun Database object and store the ID
            try:
                new_pipeline_run_created, new_pipeline_run_uuid = ETL_PipelineRunService.create_etl_pipeline_run()
                self.ETL_PipelineRun__UUID = str(new_pipeline_run_uuid).strip()
                # Log Activity - Pipeline Started
                activity_event_type     = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_STARTED", default_or_error_return_value="ETL Pipeline Started")
                activity_description    = "Starting Pipeline for Dataset: " + str(self.dataset_name)
                additional_json         = self.to_JSONable_Object()
                self.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
            except:
                raise etl_exceptions.UnableToCreatePipelineRunException()

            # Read dataset info from the database
            try:
                self.dataset = ETL_Dataset.objects.get(pk=self.etl_dataset_uuid)
                self.dataset_JSONable_Object = ETL_DatasetSerializer(self.dataset).data
                self.dataset_name = self.dataset_JSONable_Object['dataset_name']
            except:
                raise etl_exceptions.UnableToReadDatasetException()

            # Get the Dataset SubType and alidate that the dataset subtype is NOT Blank
            dataset_subtype = str(self.dataset_JSONable_Object['dataset_subtype']).strip()
            if dataset_subtype == '':
                raise etl_exceptions.BlankDatasetSubtypeException()

            # Validate that the dataset subtype is valid
            is_valid_subtype = ETL_DatasetService.is_a_valid_subtype_string(input__string=dataset_subtype)
            if is_valid_subtype == False:
                raise etl_exceptions.InvalidDatasetSubtypeException()

            # Create dataset instance
            if dataset_subtype in ('chirp', 'chirps'):
                self.Subtype_ETL_Instance = ETL_Dataset_Subtype_CHIRPS(self, dataset_subtype)
            elif dataset_subtype == 'chirps_gefs':
                self.Subtype_ETL_Instance = ETL_Dataset_Subtype_CHIRPS_GEFS(self, dataset_subtype)
            elif dataset_subtype == 'emodis':
                self.Subtype_ETL_Instance = ETL_Dataset_Subtype_EMODIS(self)
            elif dataset_subtype in ('esi_4week', 'esi_12week'):
                self.Subtype_ETL_Instance = ETL_Dataset_Subtype_ESI(self, dataset_subtype)
            elif dataset_subtype in ('imerg_early_30min', 'imerg_late_30min', 'imerg_early_1dy', 'imerg_late_1dy'):
                self.Subtype_ETL_Instance = ETL_Dataset_Subtype_IMERG(self, dataset_subtype)
            elif dataset_subtype == 'nmme':
                self.Subtype_ETL_Instance = ETL_Dataset_Subtype_NMME(self)
            elif dataset_subtype == 'nmme_cfsv2':
                self.Subtype_ETL_Instance = ETL_Dataset_Subtype_NMME_CFSV2(self)
            elif dataset_subtype == 'usda_smap':
                self.Subtype_ETL_Instance = ETL_Dataset_Subtype_USDA_SMAP(self, dataset_subtype)
            elif dataset_subtype in ('esi_4week_servir', 'esi_12week_servir'):
                self.Subtype_ETL_Instance = ETL_Dataset_Subtype_ESI_SERVIR(self, dataset_subtype)
            elif dataset_subtype == 'nsidc_smap_9km':
                self.Subtype_ETL_Instance = ETL_Dataset_Subtype_NSIDC_SMAP_9KM(self, dataset_subtype)
            elif dataset_subtype == 'nsidc_smap_36km':
                self.Subtype_ETL_Instance = ETL_Dataset_Subtype_NSIDC_SMAP_36KM(self, dataset_subtype)
            else:
                raise etl_exceptions.InvalidDatasetSubtypeException()

            # Way to determinate which is the last processed file to use its date as starting date
            if self.from_last_processed:
                final_load_dir = self.dataset.final_load_dir
                list_of_files = sorted(filter(os.path.isfile, glob.glob(final_load_dir + '/**/*', recursive=True)))
                if len(list_of_files) != 0:
                    last_processed_file = list_of_files[-1]
                    date = os.path.basename(last_processed_file).split('.')
                    if len(date) > 0:
                        self.START_YEAR_YYYY = int(date[1][:4])
                        self.START_MONTH_MM = int(date[1][4:6])
                        self.START_DAY_DD = int(date[1][6:8])

            # Set optional params
            self.Subtype_ETL_Instance.set_optional_parameters({
                'no_duplicates': self.no_duplicates,
                'merge_yearly': self.merge_yearly,
                'merge_monthly': self.merge_monthly,
                'YYYY__Year__Start': self.START_YEAR_YYYY,
                'YYYY__Year__End': self.END_YEAR_YYYY,
                'MM__Month__Start': self.START_MONTH_MM,
                'MM__Month__End': self.END_MONTH_MM,
                'DD__Day__Start': self.START_DAY_DD,
                'DD__Day__End': self.END_DAY_DD
            })

        except etl_exceptions.UnableToReadDatasetException:
            sysErrorData            = str(sys.exc_info())
            activity_event_type     = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
            activity_description    = "Unable to start pipeline.  Error Reading dataset (etl_dataset_uuid) " + self.etl_dataset_uuid + " from the database: Sys Error Message: " + str(sysErrorData)
            additional_json         = self.to_JSONable_Object()
            self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)

        except etl_exceptions.UnableToCreatePipelineRunException:
            # Log the Error (Unable to Create New Database Object for this Pipeline Run - This means something may be wrong with the database or the connection to the database.  This must be fixed for all of the below steps to work proplery.)
            sysErrorData            = str(sys.exc_info())
            activity_event_type     = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
            activity_description    = "Unable to Create New Database Object for this Pipeline Run - This means something may be wrong with the database or the connection to the database.  This must be fixed for all of the below steps to work properly.   For Tracking Purposes: The Dataset UUID for this Error (etl_dataset_uuid) " + self.etl_dataset_uuid + "  System Error Message: " + str(sysErrorData)
            additional_json         = self.to_JSONable_Object()
            self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)

        except etl_exceptions.BlankDatasetSubtypeException:
            activity_event_type = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
            activity_description = "Unable to start pipeline.  The dataset subtype was blank.  This value is required for etl pipeline operation.  This value comes from the Dataset object in the database.  To find the correct Dataset object to modify, look up the ETL_Dataset record with ID: " + str(self.etl_dataset_uuid) + " and set the dataset_subtype property to one of these values: " + str(list_of_valid__dataset_subtypes)
            additional_json = self.to_JSONable_Object()
            self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)

        except etl_exceptions.InvalidDatasetSubtypeException:
            activity_event_type = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
            activity_description = "Unable to start pipeline.  The dataset subtype was invalid.  The value tried was: '" + dataset_subtype + "'.  This value comes from the Dataset object in the database.  To find the correct Dataset object to modify, look up the ETL_Dataset record with ID: " + str(self.etl_dataset_uuid) + " and set the dataset_subtype property to one of these values: " + str(list_of_valid__dataset_subtypes)
            additional_json = self.to_JSONable_Object()
            self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)

        finally:
            self.log__pipeline_run__exit()

        # Standardized Pipeline Steps
        has_error = False  # Keeping track of if there is an error.

        # STEP: execute__Step__Pre_ETL_Custom
        if has_error == False:
            step_name = Config_Setting.get_value(setting_name="ETL_PIPELINE_STEP__PRE_ETL_CUSTOM", default_or_error_return_value="Pre ETL Custom")
            has_error, step_result = self.execute__Step__Pre_ETL_Custom()
            if has_error == True:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
                activity_description            = "An Error Occurred in the pipeline while attempting step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
                self.log__pipeline_run__exit()
                return
            else:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_STEP_COMPLETED", default_or_error_return_value="ETL Step Completed")
                activity_event_type             = activity_event_type + ": " + str(step_name)
                activity_description            = "The pipeline just completed step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

        # STEP: execute__Step__Download
        if has_error == False:
            step_name = Config_Setting.get_value(setting_name="ETL_PIPELINE_STEP__DOWNLOAD", default_or_error_return_value="ETL Download")
            has_error, step_result = self.execute__Step__Download()
            if has_error == True:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
                activity_description            = "An Error Occurred in the pipeline while attempting step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
                self.log__pipeline_run__exit()
                return
            else:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_STEP_COMPLETED", default_or_error_return_value="ETL Step Completed")
                activity_event_type             = activity_event_type + ": " + str(step_name)
                activity_description            = "The pipeline just completed step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

        # STEP: execute__Step__Extract
        if has_error == False:
            step_name = Config_Setting.get_value(setting_name="ETL_PIPELINE_STEP__EXTRACT", default_or_error_return_value="ETL Extract")
            has_error, step_result = self.execute__Step__Extract()
            if has_error == True:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
                activity_description            = "An Error Occurred in the pipeline while attempting step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
                self.log__pipeline_run__exit()
                return
            else:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_STEP_COMPLETED", default_or_error_return_value="ETL Step Completed")
                activity_event_type             = activity_event_type + ": " + str(step_name)
                activity_description            = "The pipeline just completed step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

        # STEP: execute__Step__Transform
        if has_error == False:
            step_name = Config_Setting.get_value(setting_name="ETL_PIPELINE_STEP__TRANSFORM", default_or_error_return_value="ETL Transform")
            has_error, step_result = self.execute__Step__Transform()
            if has_error == True:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
                activity_description            = "An Error Occurred in the pipeline while attempting step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
                self.log__pipeline_run__exit()
                return
            else:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_STEP_COMPLETED", default_or_error_return_value="ETL Step Completed")
                activity_event_type             = activity_event_type + ": " + str(step_name)
                activity_description            = "The pipeline just completed step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

        # STEP: execute__Step__Load
        if has_error == False:
            step_name = Config_Setting.get_value(setting_name="ETL_PIPELINE_STEP__LOAD", default_or_error_return_value="ETL Load")
            has_error, step_result = self.execute__Step__Load()
            if has_error == True:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
                activity_description            = "An Error Occurred in the pipeline while attempting step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
                self.log__pipeline_run__exit()
                return
            else:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_STEP_COMPLETED", default_or_error_return_value="ETL Step Completed")
                activity_event_type             = activity_event_type + ": " + str(step_name)
                activity_description            = "The pipeline just completed step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

        # STEP: execute__Step__Post_ETL_Custom
        if has_error == False:
            step_name = Config_Setting.get_value(setting_name="ETL_PIPELINE_STEP__POST_ETL_CUSTOM", default_or_error_return_value="Post ETL Custom")
            has_error, step_result = self.execute__Step__Post_ETL_Custom()
            if has_error == True:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
                activity_description            = "An Error Occurred in the pipeline while attempting step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
                self.log__pipeline_run__exit()
                return
            else:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_STEP_COMPLETED", default_or_error_return_value="ETL Step Completed")
                activity_event_type             = activity_event_type + ": " + str(step_name)
                activity_description            = "The pipeline just completed step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

        # STEP: execute__Step__Clean_Up
        if has_error == False:
            step_name = Config_Setting.get_value(setting_name="ETL_PIPELINE_STEP__CLEAN_UP", default_or_error_return_value="ETL Cleanup")
            has_error, step_result = self.execute__Step__Clean_Up()
            if has_error == True:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
                activity_description            = "An Error Occurred in the pipeline while attempting step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
                self.log__pipeline_run__exit()
                return
            else:
                activity_event_type             = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_STEP_COMPLETED", default_or_error_return_value="ETL Step Completed")
                activity_event_type             = activity_event_type + ": " + str(step_name)
                activity_description            = "The pipeline just completed step: {}".format(str(step_name))
                additional_json                 = self.to_JSONable_Object()
                additional_json['step_result']  = step_result
                self.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

        # Exit the pipeline
        self.log__pipeline_run__exit()
        return

    def execute__Step__Pre_ETL_Custom(self):
        has_error = False
        step_result = {}
        try:
            step_result = self.Subtype_ETL_Instance.execute__Step__Pre_ETL_Custom()
            has_error = step_result['is_error']
        except:
            sysErrorData = str(sys.exc_info())
            step_result['sys_error'] = "System Error in function: execute__Step__Pre_ETL_Custom:  System Error Message: " + str(sysErrorData)
            has_error = True
        return has_error, step_result

    def execute__Step__Download(self):
        has_error = False
        step_result = {}
        try:
            step_result = self.Subtype_ETL_Instance.execute__Step__Download()
            has_error = step_result['is_error']
        except:
            sysErrorData = str(sys.exc_info())
            step_result['sys_error'] = "System Error in function: execute__Step__Download:  System Error Message: " + str(sysErrorData)
            has_error = True
        return has_error, step_result

    def execute__Step__Extract(self):
        has_error = False
        step_result = {}
        try:
            step_result = self.Subtype_ETL_Instance.execute__Step__Extract()
            has_error = step_result['is_error']
        except:
            sysErrorData = str(sys.exc_info())
            step_result['sys_error'] = "System Error in function: execute__Step__Extract:  System Error Message: " + str(sysErrorData)
            has_error = True
        return has_error, step_result

    def execute__Step__Transform(self):
        has_error = False
        step_result = {}
        try:
            step_result = self.Subtype_ETL_Instance.execute__Step__Transform()
            has_error = step_result['is_error']
        except:
            sysErrorData = str(sys.exc_info())
            step_result['sys_error'] = "System Error in function: execute__Step__Transform:  System Error Message: " + str(sysErrorData)
            has_error = True
        return has_error, step_result

    def execute__Step__Load(self):
        has_error = False
        step_result = {}
        try:
            step_result = self.Subtype_ETL_Instance.execute__Step__Load()
            has_error = step_result['is_error']
        except:
            sysErrorData = str(sys.exc_info())
            step_result['sys_error'] = "System Error in function: execute__Step__Load:  System Error Message: " + str(sysErrorData)
            has_error = True
        return has_error, step_result

    def execute__Step__Post_ETL_Custom(self):
        has_error = False
        step_result = {}
        try:
            step_result = self.Subtype_ETL_Instance.execute__Step__Post_ETL_Custom()
            has_error = step_result['is_error']
        except:
            sysErrorData = str(sys.exc_info())
            step_result['sys_error'] = "System Error in function: execute__Step__Post_ETL_Custom:  System Error Message: " + str(sysErrorData)
            has_error = True
        return has_error, step_result

    def execute__Step__Clean_Up(self):
        has_error = False
        step_result = {}
        try:
            step_result = self.Subtype_ETL_Instance.execute__Step__Clean_Up()
            has_error = step_result['is_error']
        except:
            sysErrorData = str(sys.exc_info())
            step_result['sys_error'] = "System Error in function: execute__Step__Clean_Up:  System Error Message: " + str(sysErrorData)
            has_error = True
        return has_error, step_result
