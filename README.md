# ClimateSERV 2.0

[![Django: 3.x](https://img.shields.io/badge/Django-3.x-blue)](https://www.djangoproject.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![SERVIR: Global](https://img.shields.io/badge/SERVIR-Global-green)](https://servirglobal.net)

This tool allows development practitioners, scientists/researchers, and government decision-makers to visualize and
download historical rainfall data, vegetation condition data, and 180-day forecasts of rainfall and temperature to
improve understanding of, and make improved decisions for, issues related to agriculture and water availability.
These data can be accessed directly through the web application or can be accessed through the applications API
using ClimateSERVpy in your python application.  You can get the ClimateSERVpy python package using pip 
```shell
pip install climateserv
```
or conda
```shell
conda install -c servir climateserv
```

## Setup and Installation

### Required

- [Python 3.9.5 or greater](https://www.python.org/downloads/) (We suggest 3.9.5)
- [Postgresql (version 13)](https://www.postgresql.org/download)
- [THREDDS (version 4.6.14)](https://www.unidata.ucar.edu/software/tds/)

### Environment
These instructions are written for a linux environment.  If you are installing on a different
OS you will have to alter pathing and pathing variables.

We highly recommend following the structure we have in place for the application, which is:

```/cserv2/django_app/ClimateSERV2```

Following these directions should help to accomplish this effectively and ensure your application runs successfully.

``` 
mkdir -p /cserv2/django_app
mkdir -p /cserv2/tmp/logs
cd /cserv2/django_app
git clone git@github.com:SERVIR/ClimateSERV2.git
```



Due to python package requirements we found it best to use a conda environment for ClimateSERV.

We use two environments, one for the database, and the other for the application.

#### To setup the database in a conda environment:
- Create the env

```
mkdir -p /cserv2/python_environments/conda/anaconda3/envs/
conda create --prefix=/cserv2/python_environments/conda/anaconda3/envs/psqlenv python=3.9.5
conda create --name psqlenv python=3.9.5
```

- enter the environment

```
conda activate psqlenv
```

- install postgresql via conda

```
conda install -y -c conda-forge postgresql
```

- create the base database

``` 
cd /cserv2
mkdir db
cd db

initdb -D local_climateserv
```

- start the server modus/instance of postgres

``` 
pg_ctl -D local_climateserv -l logfile start
```

- create a non-superuser (more safety!)

``` 
createuser --encrypted --pwprompt csadmin
```

- using this user, create inner database inside the base database

``` 
createdb --owner=csadmin cs2_db

conda deactivate
```

- Create database connection file
Create /cserv2/django_app/ClimateSERV2/climateserv2/data.json
Paste the following connection properties inside it.  For the SECRET_KEY  
must be a large random value, and it must be kept secret.  For DEBUG, in 
development, set it to "True", for production set it to "False"

``` 
{
  "NAME": "cs2_db",
  "USER": "csadmin",
  "PASSWORD": "PASSWORD_YOU_SET_FOR_THIS_USER",
  "HOST": "127.0.0.1",
  "SECRET_KEY": "Your_super_secret_key_for_django"
  "DEBUG": "False"
}
```

#### To setup the application in a conda environment
With conda installed you should be able to run the following command to create the correct python 
environment, then activate it.

```
conda create --prefix=/cserv2/python_environments/conda/anaconda3/envs/climateserv2 python=3.9.5
conda activate ClimateSERV2
```

You will need to cd to your project directory, then run the following two installation commands.
Be sure to complete the installations before moving on, they can take a little time, be patient.

```
pip install -r requirements.txt
conda install --file conda_requirements.txt
```

Upon completion, you will need to add one more global lib to broker the message queues.  To do this
use 
```shell
sudo apt-get install rabbitmq-server
sudo systemctl enable rabbitmq-server
sudo service rabbitmq-server start

```

Now we can create a service and config file to start celery which is our task queue with focus on real-time processing.
You will need to create two files, one at /etc/systemd/system/celery.service and the other at /etc/conf.d/celery
In celery.service you will need the following (you may need to adjust paths)

```shell
[Unit]
Description=Celery Service
After=rabbitmq-server.service network.target
Requires=rabbitmq-server.service
RuntimeDirectory=celery 


[Service]
Type=forking
User=www-data
Group=www-data
EnvironmentFile=/etc/conf.d/celery
WorkingDirectory=/cserv2/django_app/ClimateSERV2
ExecStart=/bin/bash -c '${CELERY_BIN} -A $CELERY_APP multi start $CELERYD_NODES \
    --pidfile=${CELERYD_PID_FILE} --logfile=${CELERYD_LOG_FILE} \
    --loglevel="${CELERYD_LOG_LEVEL}" $CELERYD_OPTS' 
ExecStop=/bin/sh -c '${CELERY_BIN} multi stopwait $CELERYD_NODES \
    --pidfile=${CELERYD_PID_FILE} --logfile=${CELERYD_LOG_FILE} \
    --loglevel="${CELERYD_LOG_LEVEL}"'
ExecReload=/bin/sh -c '${CELERY_BIN} -A $CELERY_APP multi restart $CELERYD_NODES \
    --pidfile=${CELERYD_PID_FILE} --logfile=${CELERYD_LOG_FILE} \
    --loglevel="${CELERYD_LOG_LEVEL}" $CELERYD_OPTS'
Restart=always

[Install]
WantedBy=multi-user.target
```
And in /etc/conf.d/celery you can paste
```shell
CELERYD_NODES="w1 w2 w3"
DJANGO_SETTINGS_MODULE="climateserv2.settings"

# Absolute or relative path to the 'celery' command:
CELERY_BIN="/cserv2/python_environments/conda/anaconda3/envs/climateserv2/bin/celery"

# App instance to use
CELERY_APP="climateserv2"
CELERYD_MULTI="multi"

# Extra command-line arguments to the worker
CELERYD_OPTS="--time-limit=300 --concurrency=8"
CELERYD_PID_FILE="/var/run/celery/%n.pid"
CELERYD_LOG_FILE="/var/log/celery/%n%I.log"

```

Celery uses a temp directory that will need to be automatically created on reboot, to do this
create /usr/lib/tmpfiles.d and paste the following inside:

```shell
D /var/run/celery 0777 root root - -
```

Create this directory now manually, along with a couple others:
```shell
sudo mkdir /var/run/celery 
sudo chmod 777 /var/run/celery -R

sudo mkdir /opt/celery
sudo chmod 777 /opt/celery

sudo mkdir /var/log/celery
sudo chmod 777 /var/log/celery -R
```

You should be able to enable and start celery now

```shell
sudo chmod 644 /etc/systemd/system/celery.service
sudo systemctl daemon-reload
sudo systemctl enable celery
sudo service celery restart
```

you should be able to begin application setup.  Start with 

```
python manage.py migrate
```

This will take care of setting up the database.  Next you will need a superuser, 
follow the prompts when you run 

```
python manage.py createsuperuser
```

### Application specific variables and config files:

You will need to add some paths and dataset information as follows:

--- TBD ---

At this point you should be able to start the application.  From the root directory you can run the following two commands

```
python manage.py runserver
```

Of course running the application in this manor is only for development, we recommend installing
this application on a server and serving it through nginx using gunicorn.  To do this you will need to 
have both installed on your server.  There are enough resources explaining in depth how to install them,
so we will avoid duplicating this information.  We recommend adding a service to start the application
by creating a .service file located at /etc/systemd/system.  We named ours climateserv2.service
The service file will contain the following.  If you chose to follow a different structure you will
have to update the paths.

```
[Unit]
Description=ClimateSERV daemon
After=network.target

[Service]
User=nginx
Group=nginx
SocketUser=nginx
WorkingDirectory=/cserv2/django_app/ClimateSERV2/climateserv2
accesslog = "/var/log/cserv2/gunicorn.log"
errorlog = "/var/log/cserv2/gunicornerror.log"
ExecStart=/cserv2/python_environments/conda/anaconda3/envs/climateserv2/bin/gunicorn --timeout 60 --workers 5 --pythonpath '/cserv2/django_app/ClimateSERV2,/cserv2/python_environments/conda/anaconda3/envs/climateserv2/lib/python3.9/site-packages' --bind unix:/cserv2/socks/climateserv.sock wsgi:application  

[Install]
WantedBy=multi-user.target
```

NOTE: Directory for the sock must be created and owned by nginx user.

You should now be able to start the application using the service, however we will set up some alias commands to 
make things a bit easier, as permissions sometimes get in the way.  To create these you can create a new file
located at /etc/profile.d/ and name it climateserv_alias.sh.  Add the alias commands you would like from below.
If you chose to use a different structure for the application, you will have to update the paths below.

### Suggested server aliases
```
alias cs2='conda activate ClimateSERV2'

alias d='conda deactivate'

alias so='sudo chown nginx /cserv2/django_app/ClimateSERV2 -R; sudo chown -R nginx /cserv2/tmp; sudo chmod 777 /cserv2/tmp -R; sudo chmod 777 /var/log/cserv2/climateserv2.log; sudo chown -R nginx /var/log/cserv2'

alias uo='sudo chown -R ${USER} /cserv2/django_app/ClimateSERV2; sudo chown -R ${USER} /cserv2/tmp; sudo chmod 777 /cserv2/tmp -R; sudo chmod 777 /var/log/cserv2/climateserv2.log; sudo chown -R ${USER} /var/log/cserv2'

alias cstart='sudo service climateserv2 restart; sudo service nginx restart; so'

alias cstop='sudo pkill -f gunicorn; sudo service nginx stop'

alias chome='cd /cserv2/django_app/ClimateSERV2'

alias crestart='cstop; cstart;'
```

## Contact

### Software Developers

- [Billy Ashmall (NASA/USRA)](https://github.com/billyz313)
- [Roberto Fontanarosa (Former NASA/USRA)](https://github.com/rfontanarosa)
- [Alexandre Goberna (NASA/USRA)](https://github.com/agoberna)
- [Githika Tondapu (NASA/USRA)](https://github.com/gtondapu)

### Systems Engineering

- Lance Gilliland (NASA/Jacobs)
- Francisco Delgado (NASA/USRA)

### Science Team

- Brent Roberst (NASA)
- Ashutosh Limaye (NASA)
- Eric Anderson (NASA)



## License and Distribution

ClimateSERV 2.0 is distributed by SERVIR under the terms of the MIT License. See
[LICENSE](https://github.com/SERVIR/ClimateSERV2/blob/master/LICENSE) in this directory for more information.

## Privacy & Terms of Use

ClimateSERV abides to all of SERVIR's privacy and terms of use as described
at [https://servirglobal.net/Privacy-Terms-of-Use](https://servirglobal.net/Privacy-Terms-of-Use).

## Funding
Funding for this work was provided through SERVIR, a joint NASA- and USAID-led program, particularly through the Earth Science Branch at NASA Marshall Space Flight Center and the cooperative agreement 80MSFC17M0022 between NASA and USRA. 
