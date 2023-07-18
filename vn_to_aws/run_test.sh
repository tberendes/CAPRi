#!/bin/bash
source ./v-env/bin/activate
#python vn_to_parquet_partition.py /data/gpmgv_test/netcdf/geo_match/GPM/2ADPR/FS_Ku/V07A/2_1 /data/gpmgv_test/aws_athena/parquet /data/gpmgv_test/BB/GPM_rain_event_bb_km.txt 141002 141002
#
#python vn_to_parquet_partition.py /data/v7_test/GPM/2ADPR/FS_Ku/V07A/2_3 /data/v7_test/aws/parquet 191002 191002
python vn_bin_to_png.py /data/v7_test/mrms/netcdf/geo_match/GPM/2ADPR/FS_Ku/V07A/2_3 /data/v7_test/mrms/test 140101 141231
deactivate
