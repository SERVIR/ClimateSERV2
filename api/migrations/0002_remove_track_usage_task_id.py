from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone
import uuid



class Migration(migrations.Migration):

    dependencies = [
        ('api', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Track_Usage',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('unique_id', models.CharField(max_length=50)),
                ('originating_IP', models.CharField(default=None, max_length=50)),
                ('task_ID', models.CharField(default=None, max_length=50)),
                ('time_requested', models.DateTimeField(default=django.utils.timezone.now)),
                ('AOI', models.JSONField(default=None)),
                ('dataset', models.CharField(default=None, max_length=50)),
                ('start_date', models.DateTimeField(default=django.utils.timezone.now)),
                ('end_date', models.DateTimeField(default=django.utils.timezone.now)),
                ('request_type', models.CharField(default=None, max_length=50)),
                ('status', models.CharField(default=None, max_length=50)),
                ('file_size', models.IntegerField(default=0)),
            ],
        ),
    ]
