# ---------------------------------------------------------------------------------------------
#
#  read_vn.py
#
#  Description: as the file name suggests this script reads data from an GPM VN file, formatted in netCDF
#               and parses out the values and formats output for JSON
#
#  Syntax: currently no input parameters
#
#  To Do: modify to accept input parameters, such as filename and maybe the location of district coordinates
#
# ---------------------------------------------------------------------------------------------


# --Do all the necessary imports
import gzip
import statistics
import sys

import boto3 as boto3
from netCDF4 import Dataset as NetCDFFile
import json
from matplotlib.patches import Polygon
import matplotlib.path as mpltPath
import uuid
from urllib.parse import unquote_plus
import datetime
from datetime import date
from datetime import timedelta
from util.util import load_json_from_s3, update_status_on_s3

s3 = boto3.resource(
    's3')

def process_file(filename):

    with gzip.open(filename) as gz:
        with NetCDFFile('dummy', mode='r', memory=gz.read()) as nc:
            print(nc.variables)
            #nc = NetCDFFile(filename)

            atimeNearestApproach = nc.variables['atimeNearestApproach'][:]
            GR_Z = nc.variables['GR_Z'][:]  # elevAngle 	fpdim
            GR_Z_StdDev = nc.variables['GR_Z_StdDev'][:] # elevAngle 	fpdim
            GR_Z_Max = nc.variables['GR_Z_Max'][:]  # elevAngle 	fpdim
            ZFactorMeasured = nc.variables['ZFactorMeasured'][:]  # elevAngle 	fpdim
            ZFactorCorrected =  nc.variables['ZFactorCorrected'][:] # elevAngle 	fpdim
            GR_RC_rainrate = nc.variables['GR_RC_rainrate'][:]  # elevAngle 	fpdim
            GR_RC_rainrate_StdDev = nc.variables['GR_RC_rainrate_StdDev'][:]   # elevAngle 	fpdim
            GR_RC_rainrate_Max =  nc.variables['GR_RC_rainrate_Max'][:]  # elevAngle 	fpdim
            GR_RR_rainrate = nc.variables['GR_RR_rainrate'][:]   # elevAngle 	fpdim
            GR_RR_rainrate_StdDev =  nc.variables['GR_RR_rainrate_StdDev'][:]  # elevAngle 	fpdim
            GR_RR_rainrate_Max = nc.variables['GR_RR_rainrate_Max'][:]   # elevAngle 	fpdim
            PrecipRate = nc.variables['PrecipRate'][:]   # elevAngle 	fpdim
            GR_Dm = nc.variables['GR_Dm'][:]   # elevAngle 	fpdim
            GR_Dm_StdDev =  nc.variables['GR_Dm_StdDev'][:]  # elevAngle 	fpdim
            GR_Dm_Max = nc.variables['GR_Dm_Max'][:]   # elevAngle 	fpdim
            Dm = nc.variables['Dm'][:]   # elevAngle 	fpdim
            GR_HID = nc.variables['GR_HID'][:]   # elevAngle 	fpdim 	hidim
            latitude = nc.variables['latitude'][:]   # elevAngle 	fpdim
            longitude = nc.variables['longitude'][:]   # elevAngle 	fpdim
            topHeight = nc.variables['topHeight'][:]   # elevAngle 	fpdim
            bottomHeight = nc.variables['bottomHeight'][:]   # elevAngle 	fpdim
            piaFinal = nc.variables['piaFinal'][:]   # fpdim
            PrecipRateSurface =  nc.variables['PrecipRateSurface'][:]  # fpdim
            SurfPrecipTotRate =  nc.variables['SurfPrecipTotRate'][:]  # fpdim
            heightStormTop = nc.variables['heightStormTop'][:]   # fpdim
            scanNum =  nc.variables['scanNum'][:]  # fpdim
            GR_Zdr =  nc.variables['GR_Zdr'][:]  # elevAngle 	fpdim
            GR_RHOhv = nc.variables['GR_RHOhv'][:]   # elevAngle 	fpdim
            GR_Nw =  nc.variables['GR_Nw'][:]  # elevAngle 	fpdim
            GR_Nw_StdDev = nc.variables['GR_Nw_StdDev'][:]   # elevAngle 	fpdim
            GR_Nw_Max = nc.variables['GR_Nw_Max'][:]   # elevAngle 	fpdim
            Nw =  nc.variables['Nw'][:]
            elevationAngle =  nc.variables['elevationAngle'][:]  # fpdim
            BBheight =  nc.variables['BBheight'][:]  # fpdim
            BBstatus =  nc.variables['BBstatus'][:]  # fpdim
            TypePrecip =  nc.variables['TypePrecip'][:]  # fpdim

            elevationAngle = GR_HID.shape[0]
            fpdim = GR_HID.shape[1]
            hidim = GR_HID.shape[2]
            for fp in range(10):
                print ('latitude '+str(latitude[0][fp]) + ' longitude '+str(longitude[0][fp]))
#        print("district ", dist_id)
#        print("mean precip ", districtPrecipStats[dist_id]['mean'])
#        print("median precip ", districtPrecipStats[dist_id]['median'])

#    print("finished file " + key)

    gz.close()

    # reformat new json structure
#    outputJson = {'dataValues' : []}
    outputJson = []
#    for key in districtPrecipStats.keys():
#        value = districtPrecipStats[key][statType]
#        jsonRecord = {'dataElement':dataElement,'period':dateStr,'orgUnit':key,'value':value}
#        outputJson.append(jsonRecord)

    return outputJson

#    return json.dumps(districtPrecipStats)


def main():
    process_file("/media/sf_berendes/capri_test_data/VN/2019/GRtoDPR.KEOX.190807.30913.V06A.DPR.NS.1_21.nc.gz")


if __name__ == '__main__':
   main()

