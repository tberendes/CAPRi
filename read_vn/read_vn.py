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
import os
import statistics
import string
import sys
import tempfile

import boto3 as boto3
from netCDF4 import Dataset as NetCDFFile
from netCDF4 import chartostring
from numpy import ma
import urllib3
import certifi
import requests
from io import BytesIO
import random

ingest_url = 'https://6inm6whnni.execute-api.us-east-1.amazonaws.com/default/ingest_vn_data'
api_key = 'LakZ1uMrR465m1GQKoQhQ7Ig3bwr7wyPavUZ9mEc'

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
http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

def zip_string(str_data: str) -> bytes:
    btsio = BytesIO()
    g = gzip.GzipFile(fileobj=btsio, mode='w')
    g.write(bytes(str_data, 'utf8'))
    g.close()
    return btsio.getvalue()


def post_http_data(request):

    request_id = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(10))
    hdrs = {'Content-Type': 'application/json; charset=UTF-8',
            'Accept': 'application/json',
            'Content-Encoding': 'gzip',
            'x-api-key': api_key}
    data = json.dumps(request)
    zipped_data = zip_string(data)
    #r = http.request('POST', ingest_url, body=data, headers=hdrs)
    r = http.request('POST', ingest_url, body=zipped_data, headers=hdrs)

    print('r.data ',r.data)
    response = json.loads(r.data)
#    print('request ', request)
    print('response ', response)
    # Check for errors
    if 'status' not in response.keys() or response['status'] != 'upload successful':
        print('AWS Error: upload failed')
        sys.exit(1)
    return response

def process_file(filename):

    with gzip.open(filename) as gz:
        with NetCDFFile('dummy', mode='r', memory=gz.read()) as nc:
            #print(nc.variables)
            #nc = NetCDFFile(filename)

            varDict_elev_fpdim = {}
            # Variables indexed by elevAngle and fpdim
            varDict_elev_fpdim['GR_Z'] = nc.variables['GR_Z'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['GR_Z_StdDev'] = nc.variables['GR_Z_StdDev'][:] # elevAngle 	fpdim
            varDict_elev_fpdim['GR_Z_Max'] = nc.variables['GR_Z_Max'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['ZFactorMeasured'] = nc.variables['ZFactorMeasured'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['ZFactorCorrected'] =  nc.variables['ZFactorCorrected'][:] # elevAngle 	fpdim
            varDict_elev_fpdim['GR_RC_rainrate'] = nc.variables['GR_RC_rainrate'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['GR_RC_rainrate_StdDev'] = nc.variables['GR_RC_rainrate_StdDev'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['GR_RC_rainrate_Max'] =  nc.variables['GR_RC_rainrate_Max'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['GR_RR_rainrate'] = nc.variables['GR_RR_rainrate'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['GR_RR_rainrate_StdDev'] =  nc.variables['GR_RR_rainrate_StdDev'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['GR_RR_rainrate_Max'] = nc.variables['GR_RR_rainrate_Max'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['PrecipRate'] = nc.variables['PrecipRate'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['GR_Dm'] = nc.variables['GR_Dm'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['GR_Dm_StdDev'] =  nc.variables['GR_Dm_StdDev'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['GR_Dm_Max'] = nc.variables['GR_Dm_Max'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['Dm'] = nc.variables['Dm'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['topHeight'] = nc.variables['topHeight'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['bottomHeight'] = nc.variables['bottomHeight'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['GR_Zdr'] =  nc.variables['GR_Zdr'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['GR_RHOhv'] = nc.variables['GR_RHOhv'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['GR_Nw'] =  nc.variables['GR_Nw'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['GR_Nw_StdDev'] = nc.variables['GR_Nw_StdDev'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['GR_Nw_Max'] = nc.variables['GR_Nw_Max'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['Nw'] =  nc.variables['Nw'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['latitude'] = nc.variables['latitude'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['longitude'] = nc.variables['longitude'][:]   # elevAngle 	fpdim

            # Ground radar hydrometeor id histograms
            hid = nc.variables['GR_HID'][:]   # elevAngle 	fpdim 	hidim

            # variables indexed by fpdim (i.e. surface level or GPM footprint regardless of height
            varDict_fpdim={}
            varDict_fpdim['piaFinal'] = nc.variables['piaFinal'][:]   # fpdim
            varDict_fpdim['PrecipRateSurface'] =  nc.variables['PrecipRateSurface'][:]  # fpdim
            varDict_fpdim['SurfPrecipTotRate'] =  nc.variables['SurfPrecipTotRate'][:]  # fpdim
            varDict_fpdim['heightStormTop'] = nc.variables['heightStormTop'][:]   # fpdim
            varDict_fpdim['scanNum'] =  nc.variables['scanNum'][:]  # fpdim
            varDict_fpdim['rayNum'] =  nc.variables['rayNum'][:]  # fpdim
            varDict_fpdim['BBheight'] =  nc.variables['BBheight'][:]  # fpdim
            varDict_fpdim['BBstatus'] =  nc.variables['BBstatus'][:]  # fpdim
            varDict_fpdim['TypePrecip'] =  nc.variables['TypePrecip'][:]  # fpdim


            # Elevation angle for surface radar, handled differently
            elevationAngle =  nc.variables['elevationAngle'][:]  # elevdim
#            elevationAngle =  index of elev loop

            # time of GPM closest approach to radar site, handled differently
            closestTime = str(chartostring(nc.variables['atimeNearestApproach'][:], encoding='utf-8'))

            #print('attribs ', nc.ncattrs())
            GPM_VERSION = getattr(nc, 'DPR_Version')
            SCAN_TYPE = getattr(nc, 'DPR_ScanType')
            vn_version =  str(ma.getdata(nc.variables['version'][0]))
            #print('version ' + str(vn_version))

            GPM_SENSOR=''
            DPR_2ADPR_file = getattr(nc, 'DPR_2ADPR_file')
            DPR_2AKU_file = getattr(nc, 'DPR_2AKU_file')
            DPR_2AKA_file = getattr(nc, 'DPR_2AKA_file')
            DPR_2BCMB_file = getattr(nc, 'DPR_2BCMB_file')
            if not DPR_2ADPR_file.startswith('no_'):
                GPM_SENSOR = 'DPR'
            elif not DPR_2AKU_file.startswith('no_'):
                GPM_SENSOR = 'Ku'
            elif not DPR_2AKA_file.startswith('no_'):
                GPM_SENSOR = 'Ka'
            elif not DPR_2BCMB_file.startswith('no_'):
                GPM_SENSOR = 'DPRGMI'
            else:
                GPM_SENSOR = 'Unknown'
            GR_site = getattr(nc, 'GR_file').split('_')[0]

            # pick variable with most dimensions to define loops
            elevations = nc.variables['GR_HID'].shape[0]
            fpdim = nc.variables['GR_HID'].shape[1]
            hidim = nc.variables['GR_HID'].shape[2]

            outputJson = []
            count=0
            for elev in range(elevations-1):
                for fp in range(fpdim-1):
                    # put in not varying values
                    fp_entry={"GPM_ver": GPM_VERSION, "VN_ver": vn_version, "scan": SCAN_TYPE, "sensor": GPM_SENSOR, "GR_site": GR_site,
                              "time": closestTime, "elev":float(elevationAngle[elev])}

                    for fp_key in varDict_fpdim:
                        fp_entry[fp_key]=float(ma.getdata(varDict_fpdim[fp_key])[fp])
                    for fp_elev_key in varDict_elev_fpdim:
                        fp_entry[fp_elev_key] = float(ma.getdata(varDict_elev_fpdim[fp_elev_key][elev])[fp])
                    for id in range(hidim-1):
                        fp_entry["hid_"+str(id+1)] = int(ma.getdata(hid[elev][fp])[id])
                    #print(fp_entry)
                    #exit(0)
                    outputJson.append(fp_entry)
                    count = count + 1
                    if count >= 10:
                        break
                if count >= 10:
                    break

    gz.close()

    return outputJson

#    return json.dumps(districtPrecipStats)


def main():
    outputJson = process_file("/media/sf_berendes/capri_test_data/VN/2019/GRtoDPR.KEOX.190807.30913.V06A.DPR.NS.1_21.nc.gz")

#    tempfp = tempfile.NamedTemporaryFile(mode = 'w', delete = False)
#    with open("/home/dhis/test_vn.json", 'w') as result_file:
#        json.dump(outputJson, tempfp)

    # try:
    #     tempname = tempfile.NamedTemporaryFile(delete=False).name+".json.gz"
    #     gz = gzip.GzipFile(filename=tempname, mode='wb', compresslevel=9)
    #     data = json.dumps(outputJson)
    #     gz.write(bytes(data, 'utf-8'))
    # except (IOError, os.error) as why:
    #     print
    #     'Failed to write the file', tempname, '\n Exception:', why
    # finally:
    #     if gz is not None:
    #         gz.close()
    # print("read_vn gzipped json output file: " + tempname)

    print ("starting upload...")

#    test_json = '{"name": "test", "min_lon": -13.6, "max_lon": -10.1, "min_lat": 6.8, "max_lat": 10.1, "variable": "HQprecipitation", "start_date": "2015-08-01T00:00:00.000Z", "end_date": "2015-08-01T23:59:59.999Z"}'
#    response = post_http_data(json.loads(test_json))
    response = post_http_data(outputJson)
    print("post response: " + json.dumps(response))


if __name__ == '__main__':
   main()

# get attributes
# // global attributes:
#:DPR_Version = "V06A";
#:DPR_ScanType = "NS";
#:GV_UF_Z_field = "CZ";
#:GV_UF_ZDR_field = "DR";
#:GV_UF_KDP_field = "KD";
#:GV_UF_RHOHV_field = "RH";
#:GV_UF_RC_field = "RC";
#:GV_UF_RP_field = "RP";
#:GV_UF_RR_field = "RR";
#:GV_UF_HID_field = "FH";
#:GV_UF_D0_field = "Unspecified";
#:GV_UF_NW_field = "NW";
#:GV_UF_DM_field = "DM";
#:GV_UF_N2_field = "Unspecified";
#:DPR_2ADPR_file = "2A-CS-CONUS.GPM.DPR.V8-20180723.20190807-S190802-E191639.030913.V06A.HDF5";
#:DPR_2AKU_file = "no_2AKU_file";
#:DPR_2AKA_file = "no_2AKA_file";
#:DPR_2BCMB_file = "no_2BCMB_file";
#:GR_file = "KEOX_2019_0807_190840.cf.gz";
