#!/bin/bash
#exec > script.log 2>&1
infile=$1
variable=$2
band=1
path=$3
echo path
cd ${path}
for idate in $(cdo showdate $infile)
do
  echo $band
  gdal_translate -ot Float64 -a_srs epsg:4326 NETCDF:"$infile":"$variable" -b $band -unscale "${idate}.tif"
  band=$((band+1))
done
echo All done
