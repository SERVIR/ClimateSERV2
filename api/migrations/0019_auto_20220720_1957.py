# Generated by Django 3.2.6 on 2022-07-20 19:57

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0018_etl_dataset_v2_dataset_name'),
    ]

    operations = [
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='contact_info',
            field=models.CharField(blank=True, help_text='Data Source Contact Info', max_length=90),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='dataset_base_directory_path',
            field=models.CharField(default='UNSET', help_text='This is a field that tells the job processing code \n                                          where to look to find ALL of the NetCDF files (NC4 \n                                          files) for this dataset.  In many cases (at this time \n                                          all cases) this is the same as the setting for the \n                                          THREDDS output directory. This is an absolute path \n                                          which means the directory path should begin with /.  \n                                          The code that uses this also expects a / at the end of \n                                          the directory name.', max_length=255, verbose_name='Base Directory Path'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='dataset_name_format',
            field=models.CharField(blank=True, help_text='Dataset file name', max_length=255),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='dataset_nc4_variable_name',
            field=models.CharField(default='ndvi', help_text='To select data from an NC4 file requires a variable name. \n                              This is the field where a variable name must be set for \n                              the selection from NC4 file to work properly.', max_length=40, verbose_name='Variable Name inside NC4 File'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='dataset_subtype',
            field=models.CharField(default='Unknown_Dataset_Subtype', help_text='IMPORTANT: This setting is used by the pipeline to select which \n                     specific sub type script logic gets used to execute the ETL job. \n                     There are a set list of Subtypes, use python manage.py \n                     list_etl_dataset_subtypes to see a list of all subtypes.', max_length=90, verbose_name='Dataset Subtype'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='ensemble',
            field=models.CharField(blank=True, help_text='Ensemble', max_length=90),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='fast_directory_path',
            field=models.TextField(default='/mnt/climateserv/process_tmp/fast_chirps/', verbose_name='Fast directory Path'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='final_load_dir',
            field=models.TextField(default='', help_text='The local filesystem place to store the final NC4 files (The THREDDS monitored Directory location)', verbose_name='(Path) Local NC4 directory'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='is_lat_order_reversed',
            field=models.BooleanField(default=False, help_text='Some datasets have their lat order reversed.  The \n                                    downstream affect of this has to do with NC4 data \n                                    selection.  If this flag is set to True then we use, \n                                    latitude=slice(max_Lat, min_Lat), if it is set to false, \n                                    then the selection uses latitude=slice(min_Lat, \n                                    max_Lat) - Note, at the time of this writing, NDVI types \n                                    should be set to False'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='is_pipeline_active',
            field=models.BooleanField(default=False, help_text='Is this ETL Dataset currently being run through the ETL \n                                 Pipeline? If this is set to True, that means an ETL job is \n                                 actually running for this specific dataset and data ingestion \n                                 is currently in progress.  When a pipeline finishes (success \n                                 or error) this value should be set to False by the pipeline \n                                 code.'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='is_pipeline_enabled',
            field=models.BooleanField(default=False, help_text="Is this ETL Dataset currently set to 'enabled' for ETL \n                                  Pipeline processing?  If this is set to False, then when the \n                                  ETL job runs to process this incoming data, the ETL pipeline \n                                  for this process will be stopped before it makes an attempt. \n                                   This is intended as a way for admin to just 'turn off' or \n                                  'turn on' a specific ETL job that has been setup."),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='is_test_object',
            field=models.BooleanField(default=False, help_text='Is this Instance meant to be used ONLY for internal platform \n                             testing? (Used only for easy cleanup - DO NOT DEPEND ON FOR VALIDATION)'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='late_after',
            field=models.IntegerField(default=0, help_text='Duration in days'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='number',
            field=models.IntegerField(default=0, help_text='Datatype number'),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='source_url',
            field=models.TextField(default='', help_text='The remote location of the dataset resource', verbose_name='Remote location (URL)'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='tds_product_name',
            field=models.CharField(default='UNKNOWN_PRODUCT_NAME', help_text="The Product name as defined on the THREDDS Data Server (TDS) \n                               Conventions Document.  Example Value: 'EMODIS-NDVI'", max_length=90, verbose_name='(TDS Conventions) Product Name'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='tds_region',
            field=models.CharField(default='UNKNOWN_REGION', help_text="The Region as defined on the THREDDS Data Server (TDS) Conventions Document.  Example Value: 'Global'", max_length=90, verbose_name='(TDS Conventions) Region'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='tds_spatial_resolution',
            field=models.CharField(default='UNKNOWN_SPATIAL_RESOLUTION', help_text="The Spatial Resolution as defined on the THREDDS Data Server \n                                     (TDS) Conventions Document.  Example Value: '250m'", max_length=90, verbose_name='(TDS Conventions) Spatial Resolution'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='tds_temporal_resolution',
            field=models.CharField(default='UNKNOWN_TEMPORAL_RESOLUTION', help_text="The Temporal Resolution as defined on the THREDDS Data \n                                      Server (TDS) Conventions Document.  Example Value: '2mon'", max_length=90, verbose_name='(TDS Conventions) Temporal Resolution'),
        ),
        migrations.AddField(
            model_name='etl_dataset_v2',
            name='temp_working_dir',
            field=models.TextField(default='', help_text='The local filesystem place to store data', verbose_name='(Path) Local temp working directory'),
        ),
    ]