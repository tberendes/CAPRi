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
from json2parquet import load_json, ingest_data, write_parquet, write_parquet_dataset

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

#s3 = boto3.resource(
#    's3')

session = boto3.Session(profile_name='CAPRI')
# Any clients created from this session will use credentials
# from the [CAPRI] section of ~/.aws/credentials.
client = session.client('s3')

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

            precip_rate_thresh = nc.variables['rain_min'][...]
            print("rain_min ", precip_rate_thresh)

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
            VN_filename = os.path.basename(filename)

            # pick variable with most dimensions to define loops
            elevations = nc.variables['GR_HID'].shape[0]
            fpdim = nc.variables['GR_HID'].shape[1]
            hidim = nc.variables['GR_HID'].shape[2]

            outputJson = []
            count=0
            site_rainy_count = 0
            for fp in range(fpdim - 1):
                for elev in range(elevations - 1):
                    if varDict_elev_fpdim['PrecipRate'][elev][fp] > precip_rate_thresh:
                        site_rainy_count = site_rainy_count + 1
                        break
            print("rainy count ", site_rainy_count)
            percent_rainy = float(site_rainy_count)/float(fpdim)
            print("fpdim ", fpdim)
            percent_rainy = 100.0 * float(site_rainy_count)/float(fpdim)
            print ("percent rainy ", percent_rainy)

            for elev in range(elevations-1):
                for fp in range(fpdim-1):
                    # put in not varying values
                    fp_entry={"GPM_ver": GPM_VERSION, "VN_ver": vn_version, "scan": SCAN_TYPE, "sensor": GPM_SENSOR,
                              "GR_site": GR_site,"time": closestTime, "elev":float(elevationAngle[elev]),
                              "vn_filename":VN_filename, "site_percent_rainy":percent_rainy,
                              "site_rainy":site_rainy_count, "site_fp_count":fpdim}

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
                #     if count >= 10:
                #         break
                # if count >= 10:
                #     break

    gz.close()

    return outputJson

#    return json.dumps(districtPrecipStats)

def upload_s3(local_file, s3_bucket, s3_key):
    try:
        client.head_object(Bucket=s3_bucket, Key=s3_key)
        print("file " + s3_key + " is already in S3 bucket " + s3_bucket + ", Skipping ...")
    except:
        print("Uploading " + s3_key + " to S3 bucket " + s3_bucket + " ...")
        client.upload_file(local_file, s3_bucket, s3_key)

def main():

    # local_directory = '/media/sf_berendes/capri_test_data/VN/2019/'
    # destination = 'Folder_name'  # S3 folder inside the bucket
    # bucket = 'Bucket_name'
    # client = boto3.client('s3')
    # # enumerate local files recursively
    # for root, dirs, files in os.walk(local_directory):
    #     for filename in files:
    #         # construct the full local path
    #         local_path = os.path.join(root, filename)
    #         # construct the full Dropbox path
    #         relative_path = os.path.relpath(local_path, local_directory)
    #         s3_path = os.path.join(destination, relative_path)
    #         print('Searching "%s" in "%s"' % (s3_path, bucket))
    #         try:
    #             client.head_object(Bucket=bucket, Key=s3_path)
    #             print("Path found on S3! Skipping %s..." % s3_path)
    #         except:
    #             print("Uploading %s..." % s3_path)
    #             client.upload_file(local_path, bucket, s3_path)

    VN_DIR = '/media/sf_berendes/capri_test_data/VN/mrms_geomatch'
    OUT_DIR = '/media/sf_berendes/capri_test_data/VN_parquet'
    META_DIR = '/media/sf_berendes/capri_test_data/meta'
    s3_bucket = 'capri-data'
    #client = boto3.client('s3')

    for root, dirs, files in os.walk(VN_DIR, topdown=False):
        for file in files:
            #print('file ' + file)
            # only process zipped nc VN files
            if file.endswith('.nc.gz'):
                outputJson = process_file(os.path.join(root,file))
                # look for image files with same base filename
                # put deep learning images in S3
                # put gzipped json VN file on S3 for indexing into Athena
                parquet_data = ingest_data(outputJson)
                os.makedirs(os.path.join(OUT_DIR), exist_ok=True)
                write_parquet(parquet_data, os.path.join(OUT_DIR,file+'.parquet'), compression='snappy')

#                with open(os.path.join(OUT_DIR,file+'.json'), 'w') as json_file:
#                    json.dump(outputJson, json_file)
#                json_file.close()

                metadata = { "vn_filename":outputJson[0]["vn_filename"], "time": outputJson[0]["time"],
                             "site_rainy": outputJson[0]["site_rainy"], "site_fp_count": outputJson[0]["site_fp_count"],
                             "site_percent_rainy":outputJson[0]["site_percent_rainy"]}
                os.makedirs(os.path.join(META_DIR), exist_ok=True)
                with open(os.path.join(META_DIR,file+'.meta.json'), 'w') as json_file:
                    json.dump(metadata, json_file)
                json_file.close()

                #print("uploading metadata "+os.path.join(META_DIR,file+'.meta.json'))
                metadata_key = 'metadata/'+file+'.meta.json'
                upload_s3(os.path.join(META_DIR,file+'.meta.json'), s3_bucket, metadata_key)

                #print("uploading parquet "+os.path.join(OUT_DIR,file+'.parquet'))
                parquet_key = 'parquet/'+file+'.parquet'
                upload_s3(os.path.join(OUT_DIR,file+'.parquet'), s3_bucket, parquet_key)

                # check for GPM and MRMS DL training files (.bin)
                if os.path.isfile(os.path.join(root, file + '.gpm.bin')):
                    upload_s3(os.path.join(root,file+'.gpm.bin'), s3_bucket, 'bin/'+file+'.gpm.bin')
                if os.path.isfile(os.path.join(root, file + '.mrms.bin')):
                    upload_s3(os.path.join(root,file+'.mrms.bin'), s3_bucket, 'bin/'+file+'.mrms.bin')

                # check for GPM and MRMS DL images and kml files
                # if os.path.isfile(os.path.join(root, file + '.gpm.bw.png')):
                #     upload_s3(os.path.join(root,file+'.gpm.bw.png'), s3_bucket, 'img/'+file+'.gpm.bw.png')
                # if os.path.isfile(os.path.join(root, file + '.gpm.bw.kml')):
                #     upload_s3(os.path.join(root,file+'.gpm.bw.kml'), s3_bucket, 'img/'+file+'.gpm.bw.kml')
                if os.path.isfile(os.path.join(root, file + '.gpm.col.png')):
                    upload_s3(os.path.join(root,file+'.gpm.col.png'), s3_bucket, 'img/'+file+'.gpm.col.png')
                if os.path.isfile(os.path.join(root, file + '.gpm.col.kml')):
                    upload_s3(os.path.join(root,file+'.gpm.col.kml'), s3_bucket, 'img/'+file+'.gpm.col.kml')

                # if os.path.isfile(os.path.join(root, file + '.mrms.bw.png')):
                #     upload_s3(os.path.join(root,file+'.mrms.bw.png'), s3_bucket, 'img/'+file+'.mrms.bw.png')
                # if os.path.isfile(os.path.join(root, file + '.mrms.bw.kml')):
                #     upload_s3(os.path.join(root,file+'.mrms.bw.kml'), s3_bucket, 'img/'+file+'.mrms.bw.kml')
                if os.path.isfile(os.path.join(root, file + '.mrms.col.png')):
                    upload_s3(os.path.join(root,file+'.mrms.col.png'), s3_bucket, 'img/'+file+'.mrms.col.png')
                if os.path.isfile(os.path.join(root, file + '.mrms.col.kml')):
                    upload_s3(os.path.join(root,file+'.mrms.col.kml'), s3_bucket, 'img/'+file+'.mrms.col.kml')

#                sys.exit()


    #outputJson = process_file("/media/sf_berendes/capri_test_data/VN/2019/GRtoDPR.KEOX.190807.30913.V06A.DPR.NS.1_21.nc.gz")

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

#    print ("starting upload...")

#    test_json = '{"name": "test", "min_lon": -13.6, "max_lon": -10.1, "min_lat": 6.8, "max_lat": 10.1, "variable": "HQprecipitation", "start_date": "2015-08-01T00:00:00.000Z", "end_date": "2015-08-01T23:59:59.999Z"}'
#    response = post_http_data(json.loads(test_json))

#    response = post_http_data(outputJson)
#    print("post response: " + json.dumps(response))


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
