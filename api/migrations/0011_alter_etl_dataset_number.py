# Generated by Django 3.2.6 on 2022-04-07 03:50

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0010_etl_dataset_number'),
    ]

    operations = [
        migrations.AlterField(
            model_name='etl_dataset',
            name='number',
            field=models.IntegerField(help_text='Datatype number'),
        ),
    ]
