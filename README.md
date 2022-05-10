# ClimateSERV 2.0

[![Django: 3.x](https://img.shields.io/badge/Django-3.x-blue)](https://www.djangoproject.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![SERVIR: Global](https://img.shields.io/badge/SERVIR-Global-green)](https://servirglobal.net)

This tool allows development practitioners, scientists/researchers, and government decision-makers to visualize and
download historical rainfall data, vegetation condition data, and 180-day forecasts of rainfall and temperature to
improve understanding of, and make improved decisions for, issues related to agriculture and water availability.
These data can be accessed directly through the web application or can be accessed through the applications API
using ClimateSERVpy in your python application.  You can get the python package using pip 
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
Paste the following connection properties inside it.

``` 
{
  "NAME": "cs2_db",
  "USER": "csadmin",
  "PASSWORD": "PASSWORD_YOU_SET_FOR_THIS_USER",
  "HOST": "127.0.0.1"
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

Upon completion, you should be able to begin application setup.  Start with 

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

### Authors

- [Billy Ashmall (NASA)](mailto:billy.ashmall@nasa.gov)
- [Roberto Fontanarosa (NASA)](mailto:roberto.fontanarosa@nasa.gov)
- [Githika Tondapu (NASA)](mailto:githika.tondapu@nasa.gov)

## License and Distribution

ClimateSERV 2.0 is distributed by SERVIR under the terms of the MIT License. See
[LICENSE](https://github.com/SERVIR/ClimateSERV2/blob/master/LICENSE) in this directory for more information.

## Privacy & Terms of Use

ClimateSERV abides to all of SERVIR's privacy and terms of use as described
at [https://servirglobal.net/Privacy-Terms-of-Use](https://servirglobal.net/Privacy-Terms-of-Use).
