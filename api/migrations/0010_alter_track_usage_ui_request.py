# Generated by Django 3.2.6 on 2021-12-20 15:45

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0009_track_usage_ui_request'),
    ]

    operations = [
        migrations.AlterField(
            model_name='track_usage',
            name='ui_request',
            field=models.BooleanField(default=False),
        ),
    ]