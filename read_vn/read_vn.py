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


import csv
# --Do all the necessary imports
import gzip
import os
import random
import string
import sys
from io import BytesIO

import boto3 as boto3
import certifi
import urllib3
import json2parquet
#from json2parquet import ingest_data, write_parquet
from netCDF4 import Dataset as NetCDFFile
from netCDF4 import chartostring
import numpy as np
from numpy import ma

ingest_url = 'https://6inm6whnni.execute-api.us-east-1.amazonaws.com/default/ingest_vn_data'
api_key = 'LakZ1uMrR465m1GQKoQhQ7Ig3bwr7wyPavUZ9mEc'

import json

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

def compute_mean_BB(BBheight, BBquality, BBraintype):
    fpdim = len(BBheight)
    mean = 0.0
    count = 0
    for fp in range(fpdim-1):
        if BBheight[fp] > 0 and BBraintype[fp] == 1 and BBquality[fp] == 1:
            mean = mean + BBheight[fp]
            count = count + 1
    if count == 0:
        return -9999.0
    else:
        return mean / count
def compute_BB_prox(meanBB, top, botm):
    #print("meanBB ", meanBB, " top ", top, " bottom ", botm)
    bbwidth=0.750
    htcat = -9999
    if meanBB > 0.0:
        if botm > (meanBB+bbwidth):
            htcat = 3 # above BB
        if top > 0.0 and top < (meanBB-bbwidth):
            htcat = 1 # below BB
        if botm <= (meanBB+bbwidth) and top >= (meanBB-bbwidth):
            htcat = 2  # within BB
    return htcat
       # num_in_BB_Cat = LONARR(4)
       # idxabv = WHERE( botm GT (meanbb+bbwidth), countabv )
       # num_in_BB_Cat[3] = countabv
       # IF countabv GT 0 THEN bbProx[idxabv] = 3
       # idxblo = WHERE( ( top GT 0.0 ) AND ( top LT (meanbb-bbwidth) ), countblo )
       # num_in_BB_Cat[1] = countblo
       # IF countblo GT 0 THEN bbProx[idxblo] = 1
       # idxin = WHERE( (botm LE (meanbb+bbwidth)) AND (top GE (meanbb-bbwidth)), countin )
       # num_in_BB_Cat[2] = countin
       # IF countin GT 0 THEN bbProx[idxin] = 2

def process_file(filename, alt_bright_band):

    outputJson = []
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
            varDict_elev_fpdim['GR_Zdr'] =  nc.variables['GR_Zdr'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['GR_RHOhv'] = nc.variables['GR_RHOhv'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['GR_Nw'] =  nc.variables['GR_Nw'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['GR_Nw_StdDev'] = nc.variables['GR_Nw_StdDev'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['GR_Nw_Max'] = nc.variables['GR_Nw_Max'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['Nw'] =  nc.variables['Nw'][:]  # elevAngle 	fpdim
            varDict_elev_fpdim['latitude'] = nc.variables['latitude'][:]   # elevAngle 	fpdim
            varDict_elev_fpdim['longitude'] = nc.variables['longitude'][:]   # elevAngle 	fpdim

# # expected/rejected, beam filling param for GR and DPR
#             varDict_elev_fpdim['n_gr_z_rejected'] = nc.variables['n_gr_z_rejected'][:]   # elevAngle 	fpdim
#             varDict_elev_fpdim['n_gr_expected'] = nc.variables['n_gr_expected'][:]   # elevAngle 	fpdim
#             varDict_elev_fpdim['n_dpr_corr_z_rejected'] = nc.variables['n_dpr_corr_z_rejected'][:]   # elevAngle 	fpdim
#             varDict_elev_fpdim['n_dpr_expected'] = nc.variables['n_dpr_expected'][:]   # elevAngle 	fpdim

            have_blockage = int(ma.getdata(nc.variables['have_GR_blockage']).data)
            print("have_blockage ",have_blockage)

            # All heights are in Km AGL, to get MSL Add site elevation
#            site_elev = ma.getdata(nc.variables['site_elev'][...]).data
            site_elev = float(ma.getdata(nc.variables['site_elev']).data)
            print("site_elev ", site_elev) # km
            #exit(0)
#            varDict_elev_fpdim['topHeight'] = 1000.0 * (nc.variables['topHeight'][:] + site_elev)  # elevAngle 	fpdim
#            varDict_elev_fpdim['bottomHeight'] = 1000.0 * (nc.variables['bottomHeight'][:] + site_elev)  # elevAngle 	fpdim
            varDict_elev_fpdim['topHeight'] = nc.variables['topHeight'][:] # elevAngle 	fpdim
            varDict_elev_fpdim['bottomHeight'] = nc.variables['bottomHeight'][:] # elevAngle 	fpdim

            # Ground radar hydrometeor id histograms
            hid = nc.variables['GR_HID'][:]   # elevAngle 	fpdim 	hidim

            # variables indexed by fpdim (i.e. surface level or GPM footprint regardless of height
            varDict_fpdim={}
            varDict_fpdim['piaFinal'] = nc.variables['piaFinal'][:]   # fpdim
            varDict_fpdim['PrecipRateSurface'] =  nc.variables['PrecipRateSurface'][:]  # fpdim
            varDict_fpdim['SurfPrecipTotRate'] =  nc.variables['SurfPrecipTotRate'][:]  # fpdim
#            varDict_fpdim['heightStormTop'] = nc.variables['heightStormTop'][:]   # fpdim
            varDict_fpdim['heightStormTop'] = (nc.variables['heightStormTop'][:] / 1000.0) - site_elev  # fpdim
            varDict_fpdim['scanNum'] =  nc.variables['scanNum'][:]  # fpdim
            varDict_fpdim['rayNum'] =  nc.variables['rayNum'][:]  # fpdim
#            varDict_fpdim['BBheight'] =  nc.variables['BBheight'][:]  # fpdim
            # convert MSL to AGL
            varDict_fpdim['BBheight'] =  (nc.variables['BBheight'][:] / 1000.0) - site_elev  # fpdim
            varDict_fpdim['BBstatus'] =  nc.variables['BBstatus'][:]  # fpdim
            varDict_fpdim['TypePrecip'] =  nc.variables['TypePrecip'][:]  # fpdim
            varDict_fpdim['clutterStatus'] =  nc.variables['TypePrecip'][:]  # fpdim

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

            # extract GPM orbit number from VN filename
            orbit_number = int(VN_filename.split('.')[3])
            print("orbit number: ", orbit_number)
            if GR_site in alt_bright_band.keys() and orbit_number in alt_bright_band[GR_site].keys():
                alt_BB_height = alt_bright_band[GR_site][orbit_number]
            else:
                alt_BB_height = -9999.0

            # pick variable with most dimensions to define loops
            elevations = nc.variables['GR_HID'].shape[0]
            fpdim = nc.variables['GR_HID'].shape[1]
            hidim = nc.variables['GR_HID'].shape[2]

            count=0
            site_rainy_count = 0
            for fp in range(fpdim - 1):
                for elev in range(elevations - 1):
                    if varDict_elev_fpdim['PrecipRate'][elev][fp] >= precip_rate_thresh:
                        site_rainy_count = site_rainy_count + 1
                        break
            print("rainy count ", site_rainy_count)
            percent_rainy = float(site_rainy_count)/float(fpdim)
            print("fpdim ", fpdim)
            percent_rainy = 100.0 * float(site_rainy_count)/float(fpdim)
            print ("percent rainy ", percent_rainy)

            # compute mean BB
            meanBB = compute_mean_BB(varDict_fpdim['BBheight'], varDict_fpdim['BBstatus'],varDict_fpdim['TypePrecip'])
            if meanBB < 0.0:
                if alt_BB_height > 0.0:
                    meanBB = alt_BB_height
                    print("missing Bright band, using Ruc_0 height ", meanBB)
                else:
                    meanBB = -9999.0
                    print("missing Bright band and Ruc_0 height...")
            else:
                print("Mean Bright band ", meanBB)

            for elev in range(elevations-1):
                for fp in range(fpdim-1):
                    # only use matchup volumes > minimimum rain rate
                    if varDict_elev_fpdim['PrecipRate'][elev][fp] < precip_rate_thresh:
                        continue

                    # put in non varying and metadata values for VN file
                    fp_entry={"GPM_ver": GPM_VERSION, "VN_ver": vn_version, "scan": SCAN_TYPE, "sensor": GPM_SENSOR,
                              "GR_site": GR_site,"time": closestTime, "elev":float(elevationAngle[elev]),
                              "vn_filename":VN_filename, "site_percent_rainy":percent_rainy,
                              "site_rainy_count":site_rainy_count, "site_fp_count":fpdim, "ruc_0_height":alt_BB_height,
                              "site_elev":site_elev, "meanBB":meanBB}

                    for fp_key in varDict_fpdim:
                        fp_entry[fp_key]=float(ma.getdata(varDict_fpdim[fp_key])[fp])
                    for fp_elev_key in varDict_elev_fpdim:
                        fp_entry[fp_elev_key] = float(ma.getdata(varDict_elev_fpdim[fp_elev_key][elev])[fp])
                    for id in range(hidim-1):
                        fp_entry["hid_"+str(id+1)] = int(ma.getdata(hid[elev][fp])[id])
                    if have_blockage == 1:
                        fp_entry['GR_blockage'] = float(ma.getdata(nc.variables['GR_blockage'][elev])[fp])
                    else:
                        fp_entry['GR_blockage'] = -9999.0

                    # compute BB proximity
                    #            varDict_elev_fpdim['topHeight'] = nc.variables['topHeight'][:] # elevAngle 	fpdim
                    #               varDict_elev_fpdim['bottomHeight'] = nc.variables['bottomHeight'][:] # elevAngle 	fpdim

                    bbprox = compute_BB_prox(meanBB, float(ma.getdata(varDict_elev_fpdim['topHeight'][elev])[fp]),
                                             float(ma.getdata(varDict_elev_fpdim['bottomHeight'][elev])[fp]))
                    fp_entry['BBprox'] = bbprox

                    # compute s2ku adjusted GR_Z
                    GR_Z = float(ma.getdata(varDict_elev_fpdim['GR_Z'][elev])[fp])
                    if bbprox == 3: # above BB, snow adjustment
                        GR_Z_s2ku = 0.185074 + 1.01378 * GR_Z - 0.00189212 * GR_Z**2   # snow
                    elif bbprox == 1: # below BB, rain adjustment
                        GR_Z_s2ku = -1.50393 + 1.07274 * GR_Z + 0.000165393 * GR_Z**2  #  rain
                    else:  # within BB
                        GR_Z_s2ku = -9999.0
                    fp_entry['GR_Z_s2ku'] = GR_Z_s2ku

                    # expected/rejected, beam filling param for GR and DPR
                    gr_exp = float(ma.getdata(nc.variables['n_gr_expected'][elev])[fp])
                    gr_rej = float(ma.getdata(nc.variables['n_gr_z_rejected'][elev])[fp])
                    dpr_exp = float(ma.getdata(nc.variables['n_dpr_expected'][elev])[fp])
                    dpr_rej = float(ma.getdata(nc.variables['n_dpr_corr_z_rejected'][elev])[fp])

                    fp_entry['GR_beam'] = 100.0 * (gr_exp - gr_rej)/gr_exp
                    fp_entry['DPR_beam'] = 100.0 * (dpr_exp - dpr_rej)/dpr_exp

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

def upload_s3(local_file, s3_bucket, s3_key, overwrite):
    try:
        client.head_object(Bucket=s3_bucket, Key=s3_key)
        if overwrite:
            print("file " + s3_key + " is already in S3 bucket " + s3_bucket + ", Overwriting ...")
            client.upload_file(local_file, s3_bucket, s3_key)
        else:
            print("file " + s3_key + " is already in S3 bucket " + s3_bucket + ", Skipping ...")
    except:
        print("Uploading " + s3_key + " to S3 bucket " + s3_bucket + " ...")
        client.upload_file(local_file, s3_bucket, s3_key)

def read_alt_bb_file(filename):
    alt_bb_dict = {}
    with open(filename) as csvfile:
        readCSV = csv.reader(csvfile, delimiter='|')
        for row in readCSV:
            radar_id=row[0]
            orbit = int(row[1])
#            height = 1000.0 * float(row[2]) # in meters
            height = float(row[2]) # in km
            if radar_id not in alt_bb_dict.keys():
                alt_bb_dict[radar_id] = {}
            alt_bb_dict[radar_id][orbit] = height
    return alt_bb_dict

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
    OUT_DIR = '/media/sf_berendes/capri_test_data/VN_parquet_8_31'
    META_DIR = '/media/sf_berendes/capri_test_data/meta'
    alt_bb_file = '/media/sf_berendes/capri_test_data/BB/GPM_rain_event_bb_km.txt'
    s3_bucket = 'capri-data'
    upload_bin = False
    upload_img = False
    upload_parquet_meta = True
    overwrite_flag = True

    #client = boto3.client('s3')
    bright_band = read_alt_bb_file(alt_bb_file)

    for root, dirs, files in os.walk(VN_DIR, topdown=False):
        for file in files:
            #print('file ' + file)
            # only process zipped nc VN files
            if file.endswith('.nc.gz'):
                if upload_parquet_meta:
                    outputJson = process_file(os.path.join(root,file), bright_band)
                    # no precip volumes were found, skip file
                    if len(outputJson) == 0:
                        print("found no precip in file " + file + " skipping...")
                        continue
                    #print(outputJson)
                    parquet_data = json2parquet.ingest_data(outputJson)
                    os.makedirs(os.path.join(OUT_DIR), exist_ok=True)
                    json2parquet.write_parquet(parquet_data, os.path.join(OUT_DIR,file+'.parquet'), compression='snappy')

                    #print("uploading parquet "+os.path.join(OUT_DIR,file+'.parquet'))
                    parquet_key = 'parquet_8_31/'+file+'.parquet'
                    upload_s3(os.path.join(OUT_DIR,file+'.parquet',), s3_bucket, parquet_key,overwrite_flag)

                    with gzip.open(os.path.join(OUT_DIR,file+'.json.gz'), 'wt', encoding="ascii") as zipfile:
                        json.dump(outputJson, zipfile)
                    zipfile.close()

                    # with open(os.path.join(OUT_DIR,file+'.json'), 'w') as json_file:
                    #     json.dump(outputJson, json_file)
                    # json_file.close()

                    metadata = { "site":outputJson[0]["GR_site"],"vn_filename":outputJson[0]["vn_filename"],
                                 "time": outputJson[0]["time"],"site_rainy_count": outputJson[0]["site_rainy_count"],
                                 "site_fp_count": outputJson[0]["site_fp_count"],
                                 "site_percent_rainy":outputJson[0]["site_percent_rainy"],
                                 "meanBB":outputJson[0]["meanBB"]}
                    os.makedirs(os.path.join(META_DIR), exist_ok=True)
                    with open(os.path.join(META_DIR,file+'.meta.json'), 'w') as json_file:
                        json.dump(metadata, json_file)
                    json_file.close()

                    #print("uploading metadata "+os.path.join(META_DIR,file+'.meta.json'))
                    metadata_key = 'metadata/'+file+'.meta.json'
                    upload_s3(os.path.join(META_DIR,file+'.meta.json'), s3_bucket, metadata_key,overwrite_flag)

                # look for deep leraning training and image files with same base filename
                # put deep learning binary files and images in S3
                if upload_bin:
                    # check for GPM and MRMS DL training files (.bin)
                    if os.path.isfile(os.path.join(root, file + '.gpm.bin')):
                        upload_s3(os.path.join(root,file+'.gpm.bin'), s3_bucket, 'bin/'+file+'.gpm.bin',overwrite_flag)
                    if os.path.isfile(os.path.join(root, file + '.mrms.bin')):
                        upload_s3(os.path.join(root,file+'.mrms.bin'), s3_bucket, 'bin/'+file+'.mrms.bin',overwrite_flag)

                if upload_img:
                    # check for GPM and MRMS DL images and kml files
                    # if os.path.isfile(os.path.join(root, file + '.gpm.bw.png')):
                    #     upload_s3(os.path.join(root,file+'.gpm.bw.png'), s3_bucket, 'img/'+file+'.gpm.bw.png',overwrite_flag)
                    # if os.path.isfile(os.path.join(root, file + '.gpm.bw.kml')):
                    #     upload_s3(os.path.join(root,file+'.gpm.bw.kml'), s3_bucket, 'img/'+file+'.gpm.bw.kml',overwrite_flag)
                    if os.path.isfile(os.path.join(root, file + '.gpm.col.png')):
                        upload_s3(os.path.join(root,file+'.gpm.col.png'), s3_bucket, 'img/'+file+'.gpm.col.png',overwrite_flag)
                    if os.path.isfile(os.path.join(root, file + '.gpm.col.kml')):
                        upload_s3(os.path.join(root,file+'.gpm.col.kml'), s3_bucket, 'img/'+file+'.gpm.col.kml',overwrite_flag)

                    # if os.path.isfile(os.path.join(root, file + '.mrms.bw.png')):
                    #     upload_s3(os.path.join(root,file+'.mrms.bw.png'), s3_bucket, 'img/'+file+'.mrms.bw.png',overwrite_flag)
                    # if os.path.isfile(os.path.join(root, file + '.mrms.bw.kml')):
                    #     upload_s3(os.path.join(root,file+'.mrms.bw.kml'), s3_bucket, 'img/'+file+'.mrms.bw.kml',overwrite_flag)
                    if os.path.isfile(os.path.join(root, file + '.mrms.col.png')):
                        upload_s3(os.path.join(root,file+'.mrms.col.png'), s3_bucket, 'img/'+file+'.mrms.col.png',overwrite_flag)
                    if os.path.isfile(os.path.join(root, file + '.mrms.col.kml')):
                        upload_s3(os.path.join(root,file+'.mrms.col.kml'), s3_bucket, 'img/'+file+'.mrms.col.kml',overwrite_flag)

                #sys.exit()


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
