import os, sys

from api.services import Config_SettingService, ETL_DatasetService, ETL_LogService

from ..models import Config_Setting
from ..models import ETL_Dataset
from ..models import ETL_Granule
from ..models import ETL_PipelineRun

from ..serializers import ETL_DatasetSerializer

from .etl_dataset_subtype_esi import esi as ETL_Dataset_Subtype_ESI

class ETL_Pipeline():

    # Pipeline Run - UUID - What is the Database UUID for this ETL Pipeline Run Instance
    ETL_PipelineRun__UUID = ""  # Set when a new Database object is created.

    # Pipeline Config Params - Set Externally (only the etl_dataset_uuid param is actually required.  The rest are optional)
    etl_dataset_uuid            = ""
    START_YEAR_YYYY             = ""
    END_YEAR_YYYY               = ""
    START_MONTH_MM              = ""
    END_MONTH_MM                = ""
    START_DAY_DD                = ""
    END_DAY_DD                  = ""
    START_30MININCREMENT_NN     = ""
    END_30MININCREMENT_NN       = ""
    REGION_CODE                 = ""
    WEEKLY_JULIAN_START_OFFSET  = ""
    # TODO: If there are more Params, they go here

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
    def __init__(self):
        self.class_name = "ETL_Pipeline"

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
        # 30 Min Increment Range
        retObj["START_30MININCREMENT_NN"]   = str(self.START_30MININCREMENT_NN).strip()
        retObj["END_30MININCREMENT_NN"]     = str(self.END_30MININCREMENT_NN).strip()
        # Region Code
        retObj["REGION_CODE"] = str(self.REGION_CODE).strip()
        # Julian Date Weekly Offset
        retObj["WEEKLY_JULIAN_START_OFFSET"] = str(self.WEEKLY_JULIAN_START_OFFSET).strip()


        # Pipeline - Dataset Config Options - Set by Reading From the Database
        retObj["dataset_name"]              = str(self.dataset_name).strip()
        retObj["dataset_JSONable_Object"]   = str(self.dataset_JSONable_Object).strip()


        retObj["new_etl_log_ids__EVENTS"]   = str(self.new_etl_log_ids__EVENTS).strip()
        retObj["new_etl_log_ids__ERRORS"]   = str(self.new_etl_log_ids__ERRORS).strip()

        retObj["new_etl_granule_ids"]               = str(self.new_etl_granule_ids).strip()
        retObj["new_etl_granule_ids__ERRORS"]       = str(self.new_etl_granule_ids__ERRORS).strip()
        retObj["affected_Available_Granule_ids"]    = str(self.affected_Available_Granule_ids).strip()

        #retObj["FUTURE_PARAM"] = str(self.FUTURE_PARAM).strip()

        return retObj

    # Standard UTIL functions (Checking for and Making Directories, parsing file names, handling datetime objects, etc)

    # Utility for creating a directory if one does not exist.
    #@staticmethod
    def create_dir_if_not_exist(self, dir_path):
        ret_IsError = False
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
                #print("DONE: log_etl_event - Created New Directory: " + str(dir_path))
                #activity_event_type     = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_DIRECTORY_CREATED
                activity_event_type     = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_DIRECTORY_CREATED", default_or_error_return_value="Directory Created")
                activity_description    = "New Directory created at path: " + str(dir_path)
                additional_json         = self.to_JSONable_Object()
                self.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)
                ret_IsError = False
            except:
                # Log the Error (Unable to create a new directory)
                sysErrorData = str(sys.exc_info())
                #activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR
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
        #
        # Output something to the terminal - this can be very helpful during debugging.
        print("")
        print("  ERROR: (error description): " + str(activity_description))
        print("")
        #
        # Signal the pipeline object that errors did occur.
        self.pipeline_had_error = True

    # Standard Function to record all Attempted Granules for this pipeline run to the Database.
    # Wrapper for creating
    def log_etl_granule(self, granule_name="unknown_etl_granule_file_or_object_name", granule_contextual_information="", granule_pipeline_state="ATTEMPTING", additional_json={}):
        # granule_pipeline_state=settings.GRANULE_PIPELINE_STATE__ATTEMPTING

        self__etl_pipeline_run_uuid     = self.ETL_PipelineRun__UUID
        self__etl_dataset_uuid          = self.etl_dataset_uuid
        self__etl_dataset_name          = "ETL_PIPELINE__" + self.dataset_name
        etl_Granule_Row_UUID = ETL_Granule.create_new_ETL_Granule_row(granule_name=granule_name,
                                                                      granule_contextual_information=granule_contextual_information,
                                                                      etl_pipeline_run_uuid=self__etl_pipeline_run_uuid,
                                                                      etl_dataset_uuid=self__etl_dataset_uuid,
                                                                      granule_pipeline_state=granule_pipeline_state,
                                                                      created_by=self__etl_dataset_name,
                                                                      additional_json=additional_json)
        self.new_etl_granule_ids.append(etl_Granule_Row_UUID)
        print(etl_Granule_Row_UUID)
        return etl_Granule_Row_UUID

    # Standard Function to update the State of an individual ETL Granule's granule_pipeline_state property - (When a granule has succeeded or failed)
    # def update_existing_ETL_Granule__granule_pipeline_state(granule_uuid, new__granule_pipeline_state):
    def etl_granule__Update__granule_pipeline_state(self, granule_uuid, new__granule_pipeline_state, is_error=False):
        is_update_succeed = ETL_Granule.update_existing_ETL_Granule__granule_pipeline_state(granule_uuid=granule_uuid, new__granule_pipeline_state=new__granule_pipeline_state)
        if is_error == True:
            self.new_etl_granule_ids__ERRORS.append(granule_uuid)
            # Placing this function call here means we don't have to ever call this from the type specific classes (Custom ETL Classes)
            is_update_succeed_2 = self.etl_granule__Update__is_missing_bool_val(granule_uuid=granule_uuid, new__is_missing__Bool_Val=True)
        return is_update_succeed

    # Standard Function to update the State of an individual if it is missing from the database or not. Updates the ETL Granule's is_missing property (bool)
    # def update_existing_ETL_Granule__is_missing_bool_val(granule_uuid, new__is_missing__Bool_Val):
    def etl_granule__Update__is_missing_bool_val(self, granule_uuid, new__is_missing__Bool_Val):
        is_update_succeed = ETL_Granule.update_existing_ETL_Granule__is_missing_bool_val(granule_uuid=granule_uuid, new__is_missing__Bool_Val=new__is_missing__Bool_Val)
        return is_update_succeed

    # Standard Function for adding new JSON data to an etl_granule (Expected Use Case: if we have an error, we can attach error info as a new JSON object to the existing record)
    # def update_existing_ETL_Granule__Append_To_Additional_JSON(granule_uuid, new_json_key_to_append, sub_jsonable_object):
    def etl_granule__Append_JSON_To_Additional_JSON(self, granule_uuid, new_json_key_to_append, sub_jsonable_object):
        is_update_succeed = ETL_Granule.update_existing_ETL_Granule__Append_To_Additional_JSON(granule_uuid=granule_uuid, new_json_key_to_append=new_json_key_to_append, sub_jsonable_object=sub_jsonable_object)
        return is_update_succeed

    # Standard Function to record Available Granules to the database (This is for the Front End - Need to better define this model first)
    # def create_or_update_existing_Available_Granule_row(granule_name, granule_contextual_information, etl_pipeline_run_uuid, etl_dataset_uuid, created_by, additional_json):
    # def create_or_update_Available_Granule(self, granule_name, granule_contextual_information="", additional_json={}):
    #     self__etl_pipeline_run_uuid = self.ETL_PipelineRun__UUID
    #     self__etl_dataset_uuid = self.etl_dataset_uuid
    #     self__etl_dataset_name = "ETL_PIPELINE__" + self.dataset_name
    #     affected_Available_Granule_UUID = Available_Granule.create_or_update_existing_Available_Granule_row(granule_name=granule_name,
    #                                                                                                         granule_contextual_information=granule_contextual_information,
    #                                                                                                         etl_pipeline_run_uuid=self__etl_pipeline_run_uuid,
    #                                                                                                         etl_dataset_uuid=self__etl_dataset_uuid,
    #                                                                                                         created_by=self__etl_dataset_name,
    #                                                                                                         additional_json=additional_json)
    #     self.affected_Available_Granule_ids.append(affected_Available_Granule_UUID)
    #     return affected_Available_Granule_UUID

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
            print("================ END ---- TERMINAL DEBUG INFO ---- END ================")
            print("")

    # Execute The Pipeline based on what ever was pre-configured.
    # # This is the main control function for the ETL pipeline
    def execute_pipeline_control_function(self):

        # Create a new ETL_PipelineRun Database object and store the ID
        try:
            # Use the Django ORM to create a new database object for this Pipeline Run Instance and Save it with all of it's defaults.  (A new UUID gets generated in the model file)
            new__ETL_PipelineRun_Instance = ETL_PipelineRun()
            new__ETL_PipelineRun_Instance.save()
            self.ETL_PipelineRun__UUID = str(new__ETL_PipelineRun_Instance.uuid).strip() # Save this ID.
        except:
            # Log the Error (Unable to Create New Database Object for this Pipeline Run - This means something may be wrong with the database or the connection to the database.  This must be fixed for all of the below steps to work proplery.)
            sysErrorData            = str(sys.exc_info())
            activity_event_type     = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
            activity_description    = "Unable to Create New Database Object for this Pipeline Run - This means something may be wrong with the database or the connection to the database.  This must be fixed for all of the below steps to work properly.   For Tracking Purposes: The Dataset UUID for this Error (etl_dataset_uuid) " + self.etl_dataset_uuid + "  System Error Message: " + str(sysErrorData)
            additional_json         = self.to_JSONable_Object()
            self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
            self.log__pipeline_run__exit()
            return

        # The Steps
        # (1) Read dataset info from the database
        self.dataset_JSONable_Object = {} #dataset_info_JSON = {} #
        dataset_name = ""
        try:
            dataset = ETL_Dataset.objects.get(pk=self.etl_dataset_uuid)
            self.dataset_JSONable_Object = ETL_DatasetSerializer(dataset).data
            # self.dataset_JSONable_Object = ETL_Dataset.objects.filter(uuid=self.etl_dataset_uuid)[0].to_JSONable_Object()
            dataset_name = self.dataset_JSONable_Object['dataset_name']
        except:
            # Log the Error (Unable to read the dataset object from the database)
            sysErrorData            = str(sys.exc_info())
            activity_event_type     = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
            activity_description    = "Unable to start pipeline.  Error Reading dataset (etl_dataset_uuid) " + self.etl_dataset_uuid + " from the database: Sys Error Message: " + str(sysErrorData)
            additional_json         = self.to_JSONable_Object()
            self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
            self.log__pipeline_run__exit()
            return

        # Set Params pulled from the database
        self.dataset_name = dataset_name

        # Log Activity - Pipeline Started
        activity_event_type     = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__PIPELINE_STARTED", default_or_error_return_value="ETL Pipeline Started")
        activity_description    = "Starting Pipeline for Dataset: " + str(self.dataset_name)
        additional_json         = self.to_JSONable_Object()
        self.log_etl_event(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=False, additional_json=additional_json)

        # Get the Dataset SubType
        current_Dataset_SubType = str(self.dataset_JSONable_Object['dataset_subtype']).strip()

        # Validate that the dataset subtype is NOT Blank
        current_Dataset_SubType = str(current_Dataset_SubType).strip()
        if current_Dataset_SubType == "":
            list_of_valid__dataset_subtypes = ETL_DatasetService.get_all_subtypes_as_string_array()
            activity_event_type = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
            activity_description = "Unable to start pipeline.  The dataset subtype was blank.  This value is required for etl pipeline operation.  This value comes from the Dataset object in the database.  To find the correct Dataset object to modify, look up the ETL_Dataset record with ID: " + str(self.etl_dataset_uuid) + " and set the dataset_subtype property to one of these values: " + str(list_of_valid__dataset_subtypes)
            additional_json = self.to_JSONable_Object()
            self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
            self.log__pipeline_run__exit()
            return

        # Validate that the dataset subtype is NOT Blank
        is_valid_subtype = ETL_DatasetService.is_a_valid_subtype_string(input__string=current_Dataset_SubType)
        if is_valid_subtype == False:
            # TODO
            list_of_valid__dataset_subtypes = ETL_DatasetService.get_all_subtypes_as_string_array()
            # activity_event_type = settings.ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR
            activity_event_type = Config_SettingService.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
            activity_description = "Unable to start pipeline.  The dataset subtype was invalid.  The value tried was: '" + current_Dataset_SubType + "'.  This value comes from the Dataset object in the database.  To find the correct Dataset object to modify, look up the ETL_Dataset record with ID: " + str(self.etl_dataset_uuid) + " and set the dataset_subtype property to one of these values: " + str(list_of_valid__dataset_subtypes)
            additional_json = self.to_JSONable_Object()
            self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
            self.log__pipeline_run__exit()
            return

        # ESI 4/12 Week
        if current_Dataset_SubType in ("esi_4week", "esi_12week"):
            # Create an instance of the subtype class - this class must implement each of the pipeline functions for this to work properly.
            self.Subtype_ETL_Instance = ETL_Dataset_Subtype_ESI(self)
            # ESI is special, requires setting which mode it is in (12week or 4week)
            if current_Dataset_SubType == "esi_4week":
                self.Subtype_ETL_Instance.set_esi_mode__To__4week()
            else:
                self.Subtype_ETL_Instance.set_esi_mode__To__12week()
            # Set ESI Params
            self.Subtype_ETL_Instance.set_esi_params(
                YYYY__Year__Start=self.START_YEAR_YYYY,
                YYYY__Year__End=self.END_YEAR_YYYY,
                MM__Month__Start=self.START_MONTH_MM,
                MM__Month__End=self.END_MONTH_MM,
                N_offset_for_weekly_julian_start_date=self.WEEKLY_JULIAN_START_OFFSET
            )

        # Validate that 'self.Subtype_ETL_Instance' is NOT NONE
        if self.Subtype_ETL_Instance is None:
            activity_event_type = Config_Setting.get_value(setting_name="ETL_LOG_ACTIVITY_EVENT_TYPE__ERROR_LEVEL_ERROR", default_or_error_return_value="ETL Error")
            activity_description = "Unable to start pipeline.  Error: etl_pipeline.Subtype_ETL_Instance was set to None.  This object needs to be set to a specific subclass which implements each of the pipeline steps in order to continue.  "
            additional_json = self.to_JSONable_Object()
            self.log_etl_error(activity_event_type=activity_event_type, activity_description=activity_description, etl_granule_uuid="", is_alert=True, additional_json=additional_json)
            self.log__pipeline_run__exit()
            return

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