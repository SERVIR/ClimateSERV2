# Generated by Django 3.2.6 on 2022-02-23 18:58

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0002_alter_run_etl_etl'),
    ]

    operations = [
        migrations.AddField(
            model_name='etl_dataset',
            name='late_after',
            field=models.IntegerField(default=0, help_text='Duration in days'),
        ),
    ]