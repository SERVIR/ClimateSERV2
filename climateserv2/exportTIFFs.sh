#!/bin/bash
#exec > script.log 2>&1
infile=$1
variable=$2
band=1
path=$3
echo path
cd ${path}
for idate in $(/cserv2/python_environments/conda/anaconda3/envs/climateserv2/bin/cdo showdate $infile)
do
  echo $band
  /cserv2/python_environments/conda/anaconda3/envs/climateserv2/bin/gdal_translate -ot Float64 -a_srs epsg:4326 NETCDF:"$infile":"$variable" -b $band -unscale "${idate}.tif"
  band=$((band+1))
done
echo All done
