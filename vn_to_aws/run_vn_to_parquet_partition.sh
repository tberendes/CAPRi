#!/bin/bash
# assume this package is installed in ~/vn_to_aws
cd ~/vn_to_aws
# activate pre-installed virtual environment with requirements
source ./v-env/bin/activate
# run DPR FS_Ku
# syntax:  python vn_to_parquet_partition.py <path to root of netcdf files> <path to root of output parquet directory> <start YYMMDD> <end YYMMDD>
# example
python vn_to_parquet_partition.py /data/gpmgv/netcdf/geo_match/GPM/2ADPR/FS_Ku/V07A/2_4 /data/gpmgv/athena 160101 230630
# after FS_Ku, run mrms matched version, will overwrite some files with MRMS matched versions.
#python vn_to_parquet_partition.py /data/gpmgv/mrms/netcdf/geo_match/GPM/2ADPR/FS_Ku/V07A/2_4 /data/gpmgv/athena 160101 230630
# run DPRGMI FS and NS
# example
python vn_to_parquet_partition.py /data/gpmgv/netcdf/geo_match/GPM/2BDPRGMI/V07A/2_4 /data/gpmgv/athena 190101 230630
deactivate
