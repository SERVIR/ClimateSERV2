from django.db import models
import uuid

class ETL_Dataset_V3(models.Model):
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


    ############################################### PATHS ##############################################################


    temp_working_dir_title = '(Path) Local temp working directory'
    temp_working_dir_help = "The local filesystem place to store temporary data for ETL pipeline"
    temp_working_dir = models.TextField(temp_working_dir_title, default='', help_text=temp_working_dir_help)


    final_load_dir_title = '(Path) Local NC4 directory'
    final_load_dir_help = "The local filesystem place to store the final NC4 files (The THREDDS monitored Directory location)"
    final_load_dir = models.TextField(final_load_dir_title, default='', help_text=final_load_dir_help)


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


    late_after = models.IntegerField(default=0, help_text="Duration in days")


    number = models.IntegerField(blank=False, help_text="Datatype number")


    fast_directory_path = models.TextField('Fast directory Path', default='/mnt/climateserv/process_tmp/fast_chirps/')


    merge_type_help = """Optional field to determine merge setting. If set to 'Monthly', the dataset will be merged 
                         monthly. If set to 'Yearly', the dataset will be merged yearly. Otherwise, the dataset will 
                         not be merged."""
    merge_type = models.CharField('Merge Type', default='None', blank=True, help_text=merge_type_help, max_length=10)


    final_nc4_filename_format_string_help = """This field determines the pre-format string that specifies the final nc4 
                                               filename when populated. Sections of the string that should be formatted 
                                               should be indicated with {} in this string."""
    final_nc4_filename_format_string = models.CharField(max_length=300, blank=True, help_text=final_nc4_filename_format_string_help, default="")


    dataset_availability_help = """This value should be set to (daily, dekadal, etc) and is used in the ETL Pipeline to
                                   determine valid dates for the dataset."""
    dataset_availability = models.CharField(max_length=20, blank=False, help_text=dataset_availability_help, default="daily")


    dataset_information_help = """This field contains information pertinent to the ClimateSERV NC4 standard for this
                                  dataset."""
    # TODO Better descriptor for dataset_information
    dataset_information = models.JSONField("NC4 Attributes Data", help_text=dataset_information_help, default=dict, blank=True)


    def __str__(self):
        return '{} - {}'.format(self.dataset_name, self.dataset_subtype)

    class Meta:
        verbose_name = 'ETL Dataset V3'
        verbose_name_plural = 'ETL Datasets V3'