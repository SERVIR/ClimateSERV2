# Generated by Django 3.2.6 on 2022-07-28 19:35

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0028_etl_dataset_v2_filename_extension'),
    ]

    operations = [
        migrations.AlterField(
            model_name='etl_dataset_v2',
            name='filename_extension',
            field=models.CharField(blank=True, default='', help_text='This field determines the extension of the file to be downloaded', max_length=8),
        ),
    ]