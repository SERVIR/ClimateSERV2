# Generated by Django 3.2.6 on 2022-04-07 03:18

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0006_parameters'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='parameters',
            options={'verbose_name': 'Parameters', 'verbose_name_plural': 'Parameters'},
        ),
    ]