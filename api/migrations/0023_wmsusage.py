# Generated by Django 3.2.15 on 2023-01-12 18:39

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0022_alter_request_progress_date_created'),
    ]

    operations = [
        migrations.CreateModel(
            name='WMSUsage',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('unique_id', models.CharField(max_length=50, unique=True)),
                ('originating_IP', models.CharField(default=None, max_length=50)),
                ('time_requested', models.DateTimeField(default=django.utils.timezone.now)),
                ('ui_id', models.CharField(default=None, max_length=200)),
            ],
            options={
                'verbose_name': 'WMS Usage Record',
                'verbose_name_plural': 'WMS Usage Records',
                'ordering': ['-time_requested'],
            },
        ),
    ]