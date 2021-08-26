# ClimateSERV 2.0

[![Django: 3.x](https://img.shields.io/badge/Django-3.x-blue)](https://www.djangoproject.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![SERVIR: Global](https://img.shields.io/badge/SERVIR-Global-green)](https://servirglobal.net)

This tool allows development practitioners, scientists/researchers, and government decision-makers to visualize and
download historical rainfall data, vegetation condition data, and 180-day forecasts of rainfall and temperature to
improve understanding of, and make improved decisions for, issues related to agriculture and water availability.

## Setup and Installation

### Required

- [Python 3.9.5 or greater](https://www.python.org/downloads/)
- [Postgresql (version 13)](https://www.postgresql.org/download)
- [PostGIS (version 3)](https://postgis.net/install/)

### Environment
Due to python package requirements we found it best to use a conda environment for ClimateSERV.

With conda installed you should be able to run the following command to create the correct python 
environment, then activate it.

```
conda create -n ClimateSERV2 python=3.9.5
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

At this point you should be able to start the application.

```
python manage.py runserver
```

Of course running the application in this manor is only for development, we recommend installing
this application on a server and serving it through nginx using gunicorn.  To do this you will need to 
have both installed on your server.  There are enough resources explaining in depth how to install them,
so we will avoid duplicating this information.  We recommend adding a service to start the application
by creating a .service file located at /etc/systemd/system.  We named ours climateserv2.service
The service file will contain the following, please substitute the correct paths as mentioned below.

```
[Unit]
Description=ClimateSERV daemon
After=network.target

[Service]
User=nginx
Group=nginx
SocketUser=nginx
WorkingDirectory={REPLACE WITH PATH TO APPLICATION ROOT}/climateserv2
ExecStart={REPLACE WITH FULL PATH TO gunicorn IN YOUR CONDA ENV}/bin/gunicorn --workers 5 --pythonpath '{REPLACE WITH PATH TO APPLICATION ROOT},{REPLACE WITH FULL PATH TO YOUR CONDA ENV}/lib/python3.9/site-packages' --bind unix:{REPLACE WITH LOCATION YOU WANT THE SOCK}
climateserv.sock wsgi:application 

[Install]
WantedBy=multi-user.target
```

NOTE: Directory for the sock must be created and owned by nginx user.

You should now be able to start the application using the service, however we will setup some alias commands to 
make things a bit easier, as permissions sometimes get in the way.  To create these you can create a new file
located at /etc/profile.d/ and name it climateserv_alias.sh.  Add the alias commands you would like from below

### Suggested server aliases
```
alias cs2='conda activate ClimateSERV2'

alias d='conda deactivate'

alias so='sudo chown nginx {REPLACE WITH PATH TO APPLICATION ROOT} -R; sudo chown -R nginx  {REPLACE WITH PATH TO temp processing directory}; sudo chmod 777 {REPLACE WITH PATH TO temp processing directory} -R'

alias uo='sudo chown -R ${USER} {REPLACE WITH PATH TO APPLICATION ROOT}; sudo chown -R ${USER} {REPLACE WITH PATH TO temp processing directory}'

alias cstart='so; sudo service climateserv2 restart; sudo service nginx restart'

alias cstop='sudo pkill -f gunicorn; sudo service nginx stop'

alias chome='cd {REPLACE WITH PATH TO APPLICATION ROOT} '

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
