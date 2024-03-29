# Generated by Django 3.2.15 on 2023-01-12 18:46

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0023_wmsusage'),
    ]

    operations = [
        migrations.AddField(
            model_name='wmsusage',
            name='country_ISO',
            field=models.CharField(default="US", help_text='ISO Country Code for originating IP', max_length=2),
            preserve_default=False,
        ),
    ]
