#!/bin/bash
#exec > script.log 2>&1
infile=$1
variable=$2
band=$5
path=$3
cd ${path}
/cserv2/python_environments/conda/anaconda3/envs/climateserv2/bin/gdal_translate -ot Float64 -a_srs epsg:4326 NETCDF:"$infile":"$variable" -b $band -unscale "$4.tif"
echo All done
