#/bin/bash
# assume this package is installed in ~/vn_to_aws
cd ~/vn_to_aws
# activate pre-installed virtual environment with requirements
source ./v-env/bin/activate
# takes input .bin files created by MRMS matchup program and produces .bin and .png files for use in deep learning projects
# such as CAPRi and CIROH
# syntax:  python vn_bin_to_png.py <path to root of MRMS .bin files> <path to root of output directory for bin and png directories> <start YYMMDD> <end YYMMDD>
# example
python vn_bin_to_png.py /data/gpmgv/mrms/netcdf/geo_match/GPM/2ADPR/FS_Ku/V07A/2_4 /data/gpmgv/mrms/deeplearning 150101 231231
deactivate
~