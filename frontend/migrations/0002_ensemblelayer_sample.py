# Generated by Django 3.2.6 on 2022-02-15 21:34

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('frontend', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='ensemblelayer',
            name='sample',
            field=models.CharField(default='', help_text='Enter API ID used to identify this data layer', max_length=200),
        ),
    ]
