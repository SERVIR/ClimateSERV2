from django.db import models
from django.contrib.postgres.fields import ArrayField
import uuid

class ETL_Dataset_V2(models.Model):
    uuid = models.CharField(default=uuid.uuid4, editable=True, max_length=40, primary_key=True, auto_created=True)


    ds_name_help = """A Human Readable Custom Name to identify this dataset.  Typically 
                                expected usage would be for Admin to set this name so they can quickly 
                                understand which data set they are looking at.  They could also use 
                                the other TDS fields to understand exactly which dataset this refers 
                                to."""
    ds_name_title = "Human Readable Dataset Short Name"
    ds_name_default = "Unknown Dataset Name"
    dataset_name = models.CharField(ds_name_title, max_length=90, blank=False, default=ds_name_default, help_text=ds_name_help)


    ds_sub_title = "Dataset Subtype"
    ds_sub_default = "Unknown_Dataset_Subtype"
    ds_sub_help = """IMPORTANT: This setting is used by the pipeline to select which 
                     specific sub type script logic gets used to execute the ETL job. 
                     There are a set list of Subtypes, use python manage.py 
                     list_etl_dataset_subtypes to see a list of all subtypes."""
    dataset_subtype = models.CharField(ds_sub_title, max_length=90, blank=False, default=ds_sub_default, help_text=ds_sub_help)


    is_pipeline_enabled_help = """Is this ETL Dataset currently set to 'enabled' for ETL 
                                  Pipeline processing?  If this is set to False, then when the 
                                  ETL job runs to process this incoming data, the ETL pipeline 
                                  for this process will be stopped before it makes an attempt. 
                                   This is intended as a way for admin to just 'turn off' or 
                                  'turn on' a specific ETL job that has been setup."""
    is_pipeline_enabled = models.BooleanField(default=False, help_text= is_pipeline_enabled_help)


    is_pipeline_active_help = """Is this ETL Dataset currently being run through the ETL 
                                 Pipeline? If this is set to True, that means an ETL job is 
                                 actually running for this specific dataset and data ingestion 
                                 is currently in progress.  When a pipeline finishes (success 
                                 or error) this value should be set to False by the pipeline 
                                 code."""
    is_pipeline_active = models.BooleanField(default=False, help_text= is_pipeline_active_help)


    #capabilities = models.TextField('JSON Data', default="{}",
    #                                help_text="Set Automatically by ETL Pipeline.  Please don't touch this!  Messing "
    #                                          "with this will likely result in broken content elsewhere in the "
    #                                          "system.  This is a field to hold Dataset specific information that the "
    #                                          "clientside code may need access to in order to properly render the "
    #                                          "details from this dataset.  (In ClimateSERV 1.0, some of this was a "
    #                                          "GeoReference, Time/Date Ranges, and other information.)")


    ############################################### PATHS ##############################################################

    temp_working_dir_title = '(Path) Local temp working directory'
    temp_working_dir_help = "The local filesystem place to store data"
    temp_working_dir = models.TextField(temp_working_dir_title, default='', help_text=temp_working_dir_help)


    final_load_dir_title = '(Path) Local NC4 directory'
    final_load_dir_help = "The local filesystem place to store the final NC4 files (The THREDDS monitored Directory location)"
    final_load_dir = models.TextField(final_load_dir_title, default='', help_text=final_load_dir_help)


    source_url_help = "The remote location of the dataset resource"
    source_url = models.TextField('Remote location (URL)', default='', help_text=source_url_help)


    source_url_list_help = "The remote location of the dataset resource"
    source_url_list = ArrayField(models.TextField(default=''), blank=True, help_text=source_url_list_help, default=list)

    ##################### THREDDS Columns (Following Threads Data Server Conventions (TDS)) ############################

    tds_product_name_title = '(TDS Conventions) Product Name'
    tds_product_name_help = """The Product name as defined on the THREDDS Data Server (TDS) 
                               Conventions Document.  Example Value: 'EMODIS-NDVI'"""
    tds_product_name = models.CharField(tds_product_name_title, max_length=90, blank=False,
                                        default="UNKNOWN_PRODUCT_NAME", help_text=tds_product_name_help)

    tds_region_title = '(TDS Conventions) Region'
    tds_region_help = "The Region as defined on the THREDDS Data Server (TDS) Conventions Document.  Example Value: 'Global'"
    tds_region = models.CharField(tds_region_title, max_length=90, blank=False, default="UNKNOWN_REGION", help_text=tds_region_help)


    tds_spatial_resolution_title = '(TDS Conventions) Spatial Resolution'
    tds_spatial_resolution_help = """The Spatial Resolution as defined on the THREDDS Data Server 
                                     (TDS) Conventions Document.  Example Value: '250m'"""
    tds_spatial_resolution = models.CharField(tds_spatial_resolution_title, max_length=90, blank=False,
                                              default="UNKNOWN_SPATIAL_RESOLUTION", help_text=tds_spatial_resolution_help)

    tds_temporal_resolution_help = """The Temporal Resolution as defined on the THREDDS Data 
                                      Server (TDS) Conventions Document.  Example Value: '2mon'"""
    tds_temporal_resolution = models.CharField('(TDS Conventions) Temporal Resolution', max_length=90, blank=False,
                                               default="UNKNOWN_TEMPORAL_RESOLUTION", help_text=tds_temporal_resolution_help)


    ## New fields needed during Job Processing
    #dataset_legacy_datatype = models.CharField('Legacy datatype', max_length=5, blank=False, default="9999",
    #                                           help_text="The 'datatype' number from ClimateSERV 1.0.  This is mapped "
    #                                                     "in support of Legacy API Requests")

    ds_nc4_var_name_help = """To select data from an NC4 file requires a variable name. 
                              This is the field where a variable name must be set for 
                              the selection from NC4 file to work properly."""
    dataset_nc4_variable_name = models.CharField('Variable Name inside NC4 File', max_length=40, blank=False,
                                                 default="ndvi", help_text=ds_nc4_var_name_help)

    is_lat_order_reversed_help = """Some datasets have their lat order reversed.  The 
                                    downstream affect of this has to do with NC4 data 
                                    selection.  If this flag is set to True then we use, 
                                    latitude=slice(max_Lat, min_Lat), if it is set to false, 
                                    then the selection uses latitude=slice(min_Lat, 
                                    max_Lat) - Note, at the time of this writing, NDVI types 
                                    should be set to False"""
    is_lat_order_reversed = models.BooleanField(default=False, help_text=is_lat_order_reversed_help)


    dataset_base_directory_path_help = """This is a field that tells the job processing code 
                                          where to look to find ALL of the NetCDF files (NC4 
                                          files) for this dataset.  In many cases (at this time 
                                          all cases) this is the same as the setting for the 
                                          THREDDS output directory. This is an absolute path 
                                          which means the directory path should begin with /.  
                                          The code that uses this also expects a / at the end of 
                                          the directory name."""
    dataset_base_directory_path = models.CharField('Base Directory Path', max_length=255, blank=False, default="UNSET",
                                                   help_text=dataset_base_directory_path_help)


    late_after = models.IntegerField(default=0, help_text="Duration in days")


    contact_info = models.CharField(blank=True, help_text="Data Source Contact Info", max_length=90)


    ensemble = models.CharField(blank=True, help_text="Ensemble", max_length=90)


    number = models.IntegerField(blank=False, help_text="Datatype number")


    dataset_name_format = models.CharField(blank=True, help_text="Dataset file name", max_length=255)


    fast_directory_path = models.TextField('Fast directory Path', default='/mnt/climateserv/process_tmp/fast_chirps/')


    merge_type_help = """Optional field to determine merge setting. If set to 'Monthly', the dataset will be merged 
                         monthly. If set to 'Yearly', the dataset will be merged yearly. Otherwise, the dataset will 
                         not be merged."""
    merge_type = models.CharField('Merge Type', default='None', blank=True, help_text=merge_type_help, max_length=10)


    filename_format_string_help = """This field specifies the string to be formatted that determines the full filename
                                     when populated. Sections of the string that should be formatted should be 
                                     indicated with {} in this string."""
    filename_format_string = models.CharField(max_length=100, blank=True, help_text=filename_format_string_help, default="")


    filename_format_string_list_help = """This field specifies the string to be formatted that determines the full filename
                                          when populated. Sections of the string that should be formatted should be 
                                          indicated with {} in this string."""
    filename_format_string_list = ArrayField(models.TextField(max_length=100),
                                             help_text=filename_format_string_list_help, default=list, blank=True)


    filename_extension_help = """This field determines the extension of the file to be downloaded"""
    filename_extension = models.CharField(max_length=8, blank=True, help_text=filename_extension_help, default="")

    filename_extension_list_help = """This field determines the extension of the file to be downloaded"""
    filename_extension_list = ArrayField(models.TextField(max_length=10),
                                         help_text=filename_extension_list_help, default=list, blank=True)

    contained_variables_list_title = """Variables by Downloaded File"""
    contained_variables_list_help = """This field determines the variables contained in each file downloaded from
                                       the source. Each element shall be represented as a JSON Field. The JSONField 
                                       should have a key for each band variable, and its corresponding value should
                                       be a string representing the variable name."""
    contained_variables_list = ArrayField(models.JSONField(contained_variables_list_title, default=dict), default=list,
                                          blank=True, help_text=contained_variables_list_help)


    final_nc4_filename_format_string_help = """This field determines the pre-format string that specifies the final nc4 
                                               filename when populated. Sections of the string that should be formatted 
                                               should be indicated with {} in this string."""
    final_nc4_filename_format_string = models.CharField(max_length=300, blank=True, help_text=final_nc4_filename_format_string_help, default="")


    nc4_attribute_data_help = """This field determines the parameters that are used to format the dataset's nc4 file."""
    nc4_attribute_data = models.JSONField("NC4 Attributes Data", help_text=nc4_attribute_data_help, default=dict, blank=True)

    def __str__(self):
        return '{} - {}'.format(self.dataset_name, self.dataset_subtype)

    class Meta:
        verbose_name = 'ETL Dataset V2'
        verbose_name_plural = 'ETL Datasets V2'