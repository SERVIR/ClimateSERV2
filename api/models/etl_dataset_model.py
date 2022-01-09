from django.db import models
import uuid

class ETL_Dataset(models.Model):
    uuid = models.CharField(default=uuid.uuid4, editable=True, max_length=40, primary_key=True, auto_created=True)
    #
    dataset_name = models.CharField('Human Readable Dataset Short Name', max_length=90, blank=False,
                                    default="Unknown Dataset Name",
                                    help_text="A Human Readable Custom Name to identify this dataset.  Typically "
                                              "expected usage would be for Admin to set this name so they can quickly "
                                              "understand which data set they are looking at.  They could also use "
                                              "the other TDS fields to understand exactly which dataset this refers "
                                              "to.")
    dataset_subtype = models.CharField('Dataset Subtype', max_length=90, blank=False, default="Unknown_Dataset_Subtype",
                                       help_text="IMPORTANT: This setting is used by the pipeline to select which "
                                                 "specific sub type script logic gets used to execute the ETL job.  "
                                                 "There are a set list of Subtypes, use python manage.py "
                                                 "list_etl_dataset_subtypes to see a list of all subtypes.")
    is_pipeline_enabled = models.BooleanField(default=False,
                                              help_text="Is this ETL Dataset currently set to 'enabled' for ETL "
                                                        "Pipeline processing?  If this is set to False, then when the "
                                                        "ETL job runs to process this incoming data, the ETL pipeline "
                                                        "for this process will be stopped before it makes an attempt. "
                                                        " This is intended as a way for admin to just 'turn off' or "
                                                        "'turn on' a specific ETL job that has been setup.")
    is_pipeline_active = models.BooleanField(default=False,
                                             help_text="Is this ETL Dataset currently being run through the ETL "
                                                       "Pipeline? If this is set to True, that means an ETL job is "
                                                       "actually running for this specific dataset and data ingestion "
                                                       "is currently in progress.  When a pipeline finishes (success "
                                                       "or error) this value should be set to False by the pipeline "
                                                       "code.")
    capabilities = models.TextField('JSON Data', default="{}",
                                    help_text="Set Automatically by ETL Pipeline.  Please don't touch this!  Messing "
                                              "with this will likely result in broken content elsewhere in the "
                                              "system.  This is a field to hold Dataset specific information that the "
                                              "clientside code may need access to in order to properly render the "
                                              "details from this dataset.  (In ClimateSERV 1.0, some of this was a "
                                              "GeoReference, Time/Date Ranges, and other information.)")
    # Paths
    temp_working_dir = models.TextField('(Path) Local temp working directory', default='',
                                        help_text="The local filesystem place to store data")
    final_load_dir = models.TextField('(Path) Local NC4 directory', default='',
                                      help_text="The local filesystem place to store the final NC4 files (The THREDDS "
                                                "monitored Directory location)")
    source_url = models.TextField('(URL) Remote location', default='', help_text="The remote location")
    # THREDDS Columns (Following Threads Data Server Conventions (TDS))
    tds_product_name = models.CharField('(TDS Conventions) Product Name', max_length=90, blank=False,
                                        default="UNKNOWN_PRODUCT_NAME",
                                        help_text="The Product name as defined on the THREDDS Data Server (TDS) "
                                                  "Conventions Document.  Example Value: 'EMODIS-NDVI'")
    tds_region = models.CharField('(TDS Conventions) Region', max_length=90, blank=False, default="UNKNOWN_REGION",
                                  help_text="The Region as defined on the THREDDS Data Server (TDS) Conventions "
                                            "Document.  Example Value: 'Global'")
    tds_spatial_resolution = models.CharField('(TDS Conventions) Spatial Resolution', max_length=90, blank=False,
                                              default="UNKNOWN_SPATIAL_RESOLUTION",
                                              help_text="The Spatial Resolution as defined on the THREDDS Data Server "
                                                        "(TDS) Conventions Document.  Example Value: '250m'")
    tds_temporal_resolution = models.CharField('(TDS Conventions) Temporal Resolution', max_length=90, blank=False,
                                               default="UNKNOWN_TEMPORAL_RESOLUTION",
                                               help_text="The Temporal Resolution as defined on the THREDDS Data "
                                                         "Server (TDS) Conventions Document.  Example Value: '2mon'")
    # New fields needed during Job Processing
    dataset_legacy_datatype = models.CharField('Legacy datatype', max_length=5, blank=False, default="9999",
                                               help_text="The 'datatype' number from ClimateSERV 1.0.  This is mapped "
                                                         "in support of Legacy API Requests")
    dataset_nc4_variable_name = models.CharField('Variable Name inside NC4 File', max_length=40, blank=False,
                                                 default="ndvi",
                                                 help_text="To select data from an NC4 file requires a variable name. "
                                                           " This is the field where a variable name must be set for "
                                                           "the selection from NC4 file to work properly.")
    is_lat_order_reversed = models.BooleanField(default=False,
                                                help_text="Some datasets have their lat order reversed.  The "
                                                          "downstream affect of this has to do with NC4 data "
                                                          "selection.  If this flag is set to True then we use, "
                                                          "latitude=slice(max_Lat, min_Lat), if it is set to false, "
                                                          "then the selection uses latitude=slice(min_Lat, "
                                                          "max_Lat) - Note, at the time of this writing, NDVI types "
                                                          "should be set to False")
    dataset_base_directory_path = models.CharField('Base Directory Path', max_length=255, blank=False, default="UNSET",
                                                   help_text="This is a field that tells the job processing code "
                                                             "where to look to find ALL of the NetCDF files (NC4 "
                                                             "files) for this dataset.  In many cases (at this time "
                                                             "all cases) this is the same as the setting for the "
                                                             "THREDDS output directory. This is an absolute path "
                                                             "which means the directory path should begin with /.  "
                                                             "The code that uses this also expects a / at the end of "
                                                             "the directory name.")
    #
    additional_json = models.TextField('JSON Data', default="{}",
                                       help_text="Extra data field.  Please don't touch this!  Messing with this will "
                                                 "likely result in broken content elsewhere in the system.")
    created_at = models.DateTimeField('created_at', auto_now_add=True, blank=True)
    created_by = models.CharField('Created By User or Process Name or ID', max_length=90, blank=False,
                                  default="Table_Default_Process",
                                  help_text="Who or What Process created this record? 90 chars max")
    is_test_object = models.BooleanField(default=False,
                                         help_text="Is this Instance meant to be used ONLY for internal platform "
                                                   "testing? (Used only for easy cleanup - DO NOT DEPEND ON FOR "
                                                   "VALIDATION)")
    start_year = models.CharField(max_length=4, blank=False)
    end_year = models.CharField(max_length=4, blank=False)
    start_month = models.CharField(max_length=2, blank=False)
    end_month = models.CharField(max_length=2, blank=False)
    start_day = models.CharField(max_length=2, blank=False)
    end_day = models.CharField(max_length=2, blank=False)

    def __str__(self):
        return '{} - {}'.format(self.dataset_name, self.dataset_subtype)

    class Meta:
        verbose_name = 'ETL Dataset'
        verbose_name_plural = 'ETL Datasets'
        ordering = ['dataset_name']
