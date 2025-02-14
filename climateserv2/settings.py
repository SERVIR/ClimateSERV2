import os
from pathlib import Path
import json

CELERY_TASK_ANNOTATIONS = {'*': {'rate_limit': '1/s'}}
CELERY_TASK_QUEUE_MAX_PRIORITY = 10
CELERY_TASK_DEFAULT_PRIORITY = 5
CELERY_ACKS_LATE = True
CELERYD_PREFETCH_MULTIPLIER = 1
CCELERY_BROKER_POOL_LIMIT = None
CELERYD_NODES = 10

# Opening JSON file
f = open('/cserv2/django_app/ClimateSERV2/climateserv2/data.json', )

# returns JSON object as
# a dictionary
data = json.load(f)

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = data["SECRET_KEY"]
reCAPTCHA_KEY = data["reCAPTCHA_KEY"]
reCAPTCHA_V2_KEY = data["reCAPTCHA_V2_KEY"]

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = json.loads((data["DEBUG"]).lower())

ALLOWED_HOSTS = ['climateserv2.servirglobal.net', 'climateserv.servirglobal.net', "localhost", "127.0.0.1",
                 "192.168.1.132", "192.168.56.132", 'csdev.servirglobal.net']

CORS_ALLOWED_ORIGIN_REGEXES = [
    r".*",
]

USE_TZ = True
# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.humanize',
    'django.contrib.staticfiles',
    'django_extensions',
    'rest_framework',
    'api',
    'etl_monitor',
    'frontend',
    'system_admin',
    'django_json_widget',
]

MIDDLEWARE = [
    'django.middleware.cache.UpdateCacheMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django.middleware.cache.FetchFromCacheMiddleware',
]

CACHE_MIDDLEWARE_ALIAS = 'default'  # which cache alias to use
CACHE_MIDDLEWARE_SECONDS = 600  # number of seconds to cache a page for (TTL)
CACHE_MIDDLEWARE_KEY_PREFIX = ''  # should be used if the cache is shared across
# multiple sites that use the same Django instance

ROOT_URLCONF = 'climateserv2.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'templates')],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

STATICFILES_DIRS = [
    os.path.join(BASE_DIR, 'etl_monitor/app/dist/'),
]

WSGI_APPLICATION = 'climateserv2.wsgi.application'

# Database
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': data["NAME"],
        'USER': data["USER"],
        'PASSWORD': data["PASSWORD"],
        'HOST': data["HOST"],
        'PORT': data["PORT"],
        'TEST': {
            'NAME': 'mytestdatabase',
        },
    }
}

# Password validation
# https://docs.djangoproject.com/en/3.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
# https://docs.djangoproject.com/en/3.1/topics/i18n/

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.1/howto/static-files/

STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')
MEDIA_ROOT = os.path.join(BASE_DIR, 'media')
GEOIP_PATH = "/cserv2/django_app/ClimateSERV2/climateserv2/geolite"
MEDIA_URL = '/media/'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s, %(asctime)s, %(module)s, %(process)d, %(thread)d, %(message)s',
            'datefmt': "%d/%b/%Y %H:%M:%S"
        },
        'simple': {
            'format': '%(levelname)s, %(message)s'
        },
    },
    'filters': {

    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'file': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': '/var/log/cserv2/climateserv2.log',
            'when': 'midnight',
            'backupCount': 10,
            'formatter': 'verbose'
        }
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': True,
        }, 'climateserv2.processDataRequest': {
            'handlers': ['file'],
            'level': 'DEBUG',
            'propagate': True,
        }
        , 'request_processor': {
            'handlers': ['file'],
            'level': 'DEBUG',
            'propagate': True,
        }
    },
}
