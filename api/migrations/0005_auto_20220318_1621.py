# Generated by Django 3.2.6 on 2022-03-18 16:21

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0004_etl_dataset_contact_us'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='etl_dataset',
            name='contact_us',
        ),
        migrations.AddField(
            model_name='etl_dataset',
            name='contact_info',
            field=models.CharField(blank=True, help_text='Data Source Contact Info', max_length=90),
        ),
    ]