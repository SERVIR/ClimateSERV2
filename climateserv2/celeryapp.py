from __future__ import absolute_import, unicode_literals
import os

from celery import Celery
from celery.signals import setup_logging
from kombu import Exchange, Queue

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'climateserv2.settings')

app = Celery('climateserv2')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.conf.task_queue_max_priority = 10
app.conf.task_default_priority = 5
app.conf.task_queues = [
    Queue('tasks', Exchange('tasks'), routing_key='tasks',
          queue_arguments={'x-max-priority': 10}, )
]


@setup_logging.connect
def config_loggers(*args, **kwargs):
    from logging.config import dictConfig  # noqa
    from django.conf import settings  # noqa

    dictConfig(settings.LOGGING)


app.autodiscover_tasks()
