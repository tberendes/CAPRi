#!/bin/bash
# This script scrapes the PPS web site and downloads the parquet data to a specified output directory
echo "starting sync..."
# format wget -nv --mirror -P <output base path> -nH --no-parent --cut-dirs=4 -R html,gif,index.* https://pmm-gv.gsfc.nasa.gov/pub/gpm-validation/data/gpmgv/athena/parquet/
wget -nv --mirror -P ./ -e robots=off -U mozilla -nH --no-parent --cut-dirs=4 -R html,gif,index.* https://pmm-gv.gsfc.nasa.gov/pub/gpm-validation/data/gpmgv/athena/parquet/
