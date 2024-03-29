# Generated by Django 3.2.6 on 2022-04-07 03:28

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0007_alter_parameters_options'),
    ]

    operations = [
        migrations.AddField(
            model_name='parameters',
            name='parameters',
            field=models.TextField(default="[[0, 'max', 'Max'], [1, 'min', 'Min'], [2, 'median', 'Median'], [3, 'range', 'Range'], [4, 'sum', 'Sum'], [5, 'avg', 'Average'], [6, 'download', 'Download'], [7, 'netcdf', 'NetCDF'], [8, 'csv', 'CSV']]", verbose_name='parameters object'),
        ),
        migrations.AlterField(
            model_name='parameters',
            name='shapefileName',
            field=models.TextField(default='{}', verbose_name='shapefile JSON Data'),
        ),
    ]
