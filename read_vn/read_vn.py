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

    #print('r.data ',r.data)
    response = json.loads(r.data)
#    print('request ', request)
    #print('response ', response)
    # Check for errors
    if 'status' not in response.keys() or response['status'] != 'upload successful':
        print('AWS Error: upload failed')
        sys.exit(1)
    return response

def compute_mean_BB_DPRGMI(BBheight):
    fpdim = len(BBheight)
    mean = 0.0
    count = 0
    for fp in range(fpdim):
        if BBheight[fp] > 0:
            mean = mean + BBheight[fp]
            count = count + 1
    if count == 0:
        return -9999.0
    else:
        return mean / count
def compute_mean_BB(BBheight, BBquality, BBraintype):
    fpdim = len(BBheight)
    mean = 0.0
    count = 0
    for fp in range(fpdim):
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
    # DPR_to_athena_variables= {
    #     'GR_Z':'GR_Z',
    #     'GR_Z_StdDev':'GR_Z_StdDev',
    #     'GR_Z_Max':'GR_Z_Max',
    #     'zFactorMeasured':'zFactorMeasured',
    #     'zFactorCorrected':'zFactorCorrected',
    #     'GR_RC_rainrate':'GR_RC_rainrate',
    #     'GR_RC_rainrate_StdDev':'GR_RC_rainrate_StdDev',
    #     'GR_RC_rainrate_Max':'GR_RC_rainrate_Max',
    #     'GR_RR_rainrate':'GR_RR_rainrate',
    #     'GR_RR_rainrate_StdDev':'GR_RR_rainrate_StdDev',
    #     'GR_RR_rainrate_Max':'GR_RR_rainrate_Max',
    #     'PrecipRate':'PrecipRate',
    #     'GR_Dm':'GR_Dm',
    #     'GR_Dm_StdDev':'GR_Dm_StdDev',
    #     'GR_Dm_Max':'GR_Dm_Max',
    #     'Dm':'Dm',
    #     'GR_HID':'GR_HID',
    #     'latitude':'latitude',
    #     'longitude':'longitude',
    #     'topHeight':'topHeight',
    #     'bottomHeight':'bottomHeight',
    #     'piaFinal':'piaFinal',
    #     'PrecipRateSurface':'PrecipRateSurface',
    #     'SurfPrecipTotRate':'SurfPrecipTotRate',
    #     'heightStormTop':'heightStormTop',
    #     'TypePrecip':'TypePrecip',
    #     'scanNum':'scanNum',
    #     'GR_Zdr':'GR_Zdr',
    #     'GR_RHOhv':'GR_RHOhv',
    #     'GR_Nw':'GR_Nw',
    #     'GR_Nw_StdDev':'GR_Nw_StdDev',
    #     'GR_Nw_Max':'GR_Nw_Max',
    #     'Nw':'Nw',
    #     'BBheight':'BBheight',
    #     'clutterStatus':'clutterStatus',
    #     'n_gr_z_rejected':'n_gr_z_rejected',
    #     'n_gr_expected':'n_gr_expected',
    #     'n_dpr_corr_z_rejected':'n_dpr_corr_z_rejected',
    #     'n_dpr_expected':'n_dpr_expected',
    #     'precipTotWaterCont':'',
    #     'ruc_0_height':'ruc_0_height',
    #     'GR_Z_s2Ku':'GR_Z_s2Ku',
    #     'GR_blockage':'GR_blockage'
    # }

    outputJson = []
    try:
        with gzip.open(filename) as gz:
            with NetCDFFile('dummy', mode='r', memory=gz.read()) as nc:
                #print(nc.variables)
                #nc = NetCDFFile(filename)
                addlist=[]
                if filename.find('DPRGMI')>=0:
                    data_is_dprgmi = True
                    addlist=['_NS','_MS']
                else:
                    data_is_dprgmi = False
                    addlist=['']

                for add in addlist:
                    varDict_elev_fpdim = {}
                    # Variables indexed by elevAngle and fpdim
                    varDict_elev_fpdim['GR_Z'] = nc.variables['GR_Z'+add][:]  # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Z_StdDev'] = nc.variables['GR_Z_StdDev'+add][:] # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Z_Max'] = nc.variables['GR_Z_Max'+add][:]  # elevAngle 	fpdim
                    if data_is_dprgmi:
                        varDict_elev_fpdim['ZFactorMeasured'] = np.empty_like(varDict_elev_fpdim['GR_Z'])
                        varDict_elev_fpdim['ZFactorMeasured'][:] = -9999.0
                        #varDict_elev_fpdim['ZFactorCorrected'] = float(ma.getdata(nc.variables['GR_blockage' + add][elev])[fp])
                        shape = nc.variables['correctedReflectFactor'+add].shape
                        eld = shape[0]
                        fpd = shape[1]
                        if len(shape) == 2:
                            varDict_elev_fpdim['ZFactorCorrected'] = nc.variables['correctedReflectFactor'+add][:]  # elevAngle 	fpdim
                        else:  # MS swath has extra dimension, use only 1st dimension (ku) value
                            nkukad = nc.variables['correctedReflectFactor'+add].shape[2]
                            varDict_elev_fpdim['ZFactorCorrected'] = np.empty_like(varDict_elev_fpdim['GR_Z']) # elevAngle 	fpdim
                            for el in range(eld):
                                for fp in range(fpd):
                                    varDict_elev_fpdim['ZFactorCorrected'][el][fp] =  float(ma.getdata(nc.variables['correctedReflectFactor'+add][el])[fp][0])
                    else:
                        varDict_elev_fpdim['ZFactorMeasured'] = nc.variables['ZFactorMeasured'][:]  # elevAngle 	fpdim
                        varDict_elev_fpdim['ZFactorCorrected'] =  nc.variables['ZFactorCorrected'][:] # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RC_rainrate'] = nc.variables['GR_RC_rainrate'+add][:]  # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RC_rainrate_StdDev'] = nc.variables['GR_RC_rainrate_StdDev'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RC_rainrate_Max'] =  nc.variables['GR_RC_rainrate_Max'+add][:]  # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RR_rainrate'] = nc.variables['GR_RR_rainrate'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RR_rainrate_StdDev'] =  nc.variables['GR_RR_rainrate_StdDev'+add][:]  # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RR_rainrate_Max'] = nc.variables['GR_RR_rainrate_Max'+add][:]   # elevAngle 	fpdim

                    if data_is_dprgmi:
                        varDict_elev_fpdim['PrecipRate'] = nc.variables['precipTotRate' + add][:]  # elevAngle 	fpdim
                    else:
                        varDict_elev_fpdim['PrecipRate'] = nc.variables['PrecipRate'][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Dm'] = nc.variables['GR_Dm'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Dm_StdDev'] =  nc.variables['GR_Dm_StdDev'+add][:]  # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Dm_Max'] = nc.variables['GR_Dm_Max'+add][:]   # elevAngle 	fpdim
                    if data_is_dprgmi:
                        varDict_elev_fpdim['Dm'] = nc.variables['precipTotPSDparamHigh'+add][:]   # elevAngle 	fpdim
                    else:
                        varDict_elev_fpdim['Dm'] = nc.variables['Dm'][:]  # elevAngle 	fpdim

                    varDict_elev_fpdim['GR_Zdr'] =  nc.variables['GR_Zdr'+add][:]  # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RHOhv'] = nc.variables['GR_RHOhv'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Nw'] =  nc.variables['GR_Nw'+add][:]  # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Nw_StdDev'] = nc.variables['GR_Nw_StdDev'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Nw_Max'] = nc.variables['GR_Nw_Max'+add][:]   # elevAngle 	fpdim
                    if data_is_dprgmi:
                        #varDict_elev_fpdim['Nw'] = nc.variables['precipTotPSDparamLow' + add][:,:,0:0]  # elevAngle 	fpdim
                        eld = nc.variables['precipTotPSDparamLow' + add].shape[0]
                        fpd = nc.variables['precipTotPSDparamLow' + add].shape[1]
                        nPSDlo = nc.variables['precipTotPSDparamLow' + add].shape[2]
                        varDict_elev_fpdim['Nw'] = np.empty_like(varDict_elev_fpdim['GR_Nw']) # elevAngle 	fpdim

                        clut_shape = nc.variables['clutterStatus' + add].shape
                        # handle kuka dimension for MS swath
                        if len(clut_shape)== 3:
                            varDict_elev_fpdim['clutterStatus'] = np.empty_like(nc.variables['n_gr_expected' + add])
                        else:
                            varDict_elev_fpdim['clutterStatus'] = nc.variables['clutterStatus' + add][:]
                        for el in range(eld):
                            for fp in range(fpd):
                                #varDict_elev_fpdim['Nw'][el][fp] =  float(ma.getdata(nc.variables['precipTotPSDparamLow' + add][el])[fp][0])
                                val = float(ma.getdata(nc.variables['precipTotPSDparamLow' + add][el])[fp][0])
                                # scales log10(Nw) values from 1/m^4 to 1/m^3
                                if val > 0:
                                    varDict_elev_fpdim['Nw'][el][fp] = val - 3.0
                                if len(clut_shape)== 3: # handle kuka dimension for MS swath (use ku)
                                    varDict_elev_fpdim['clutterStatus'][el][fp] =  int(ma.getdata(nc.variables['clutterStatus' + add][el])[fp][0])
                    else:
                        varDict_elev_fpdim['Nw'] =  nc.variables['Nw'+add][:]  # elevAngle 	fpdim
                        varDict_elev_fpdim['clutterStatus'] = nc.variables['clutterStatus' + add][:]  # fpdim
                    varDict_elev_fpdim['latitude'] = nc.variables['latitude'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['longitude'] = nc.variables['longitude'+add][:]   # elevAngle 	fpdim
                    #varDict_elev_fpdim['clutterStatus'] = nc.variables['clutterStatus' + add][:]  # fpdim

                    # # expected/rejected, beam filling param for GR and DPR
        #             varDict_elev_fpdim['n_gr_z_rejected'] = nc.variables['n_gr_z_rejected'][:]   # elevAngle 	fpdim
        #             varDict_elev_fpdim['n_gr_expected'] = nc.variables['n_gr_expected'][:]   # elevAngle 	fpdim
        #             varDict_elev_fpdim['n_dpr_corr_z_rejected'] = nc.variables['n_dpr_corr_z_rejected'][:]   # elevAngle 	fpdim
        #             varDict_elev_fpdim['n_dpr_expected'] = nc.variables['n_dpr_expected'][:]   # elevAngle 	fpdim

                    have_blockage = int(ma.getdata(nc.variables['have_GR_blockage']).data)
                    #print("have_blockage ",have_blockage)

                    # All heights are in Km AGL, to get MSL Add site elevation
        #            site_elev = ma.getdata(nc.variables['site_elev'][...]).data
                    site_elev = float(ma.getdata(nc.variables['site_elev']).data)
                    #print("site_elev ", site_elev) # km
                    #exit(0)
        #            varDict_elev_fpdim['topHeight'] = 1000.0 * (nc.variables['topHeight'][:] + site_elev)  # elevAngle 	fpdim
        #            varDict_elev_fpdim['bottomHeight'] = 1000.0 * (nc.variables['bottomHeight'][:] + site_elev)  # elevAngle 	fpdim
                    varDict_elev_fpdim['topHeight'] = nc.variables['topHeight'+add][:] # elevAngle 	fpdim
                    varDict_elev_fpdim['bottomHeight'] = nc.variables['bottomHeight'+add][:] # elevAngle 	fpdim

                    if data_is_dprgmi:
                        varDict_elev_fpdim['precipTotWaterCont'] =  nc.variables['precipTotWaterCont'+add][:]  # fpdim
                    else:
                        varDict_elev_fpdim['precipTotWaterCont'] =  np.empty_like(varDict_elev_fpdim['topHeight'])
                        varDict_elev_fpdim['precipTotWaterCont'][:] =  -9999.0

                    # Ground radar hydrometeor id histograms
                    hid = nc.variables['GR_HID'+add][:]   # elevAngle 	fpdim 	hidim

                    # variables indexed by fpdim (i.e. surface level or GPM footprint regardless of height
                    varDict_fpdim={}
                    if data_is_dprgmi:
                        shape = nc.variables['pia' + add].shape
                        fpd = shape[0]
                        if len(shape) == 1:
                            varDict_fpdim['piaFinal'] = nc.variables['pia'+add][:]   # fpdim
                        else:
                            nkukad = shape[1]
                            varDict_fpdim['piaFinal'] = np.empty_like(nc.variables['DPRlatitude'+add]) # elevAngle 	fpdim
                            for fp in range(fpd):
                                varDict_fpdim['piaFinal'][fp] =  float(ma.getdata(nc.variables['pia'+add])[fp][0])

                        #varDict_elev_fpdim['ZFactorMeasured'] = np.empty_like(varDict_elev_fpdim['GR_Z'])
                        #varDict_elev_fpdim['ZFactorMeasured'][:] = -9999.0
                        varDict_fpdim['PrecipRateSurface'] =  np.empty_like(varDict_fpdim['piaFinal'])
                        varDict_fpdim['PrecipRateSurface'][:] = -9999.0
                        varDict_fpdim['SurfPrecipTotRate'] =  nc.variables['surfPrecipTotRate'+add][:]  # fpdim
                        varDict_fpdim['heightStormTop'] = np.empty_like(varDict_fpdim['piaFinal'])
                        varDict_fpdim['heightStormTop'][:] = -9999.0
                    else:
                        varDict_fpdim['piaFinal'] = nc.variables['piaFinal'][:]  # fpdim
                        varDict_fpdim['PrecipRateSurface'] =  nc.variables['PrecipRateSurface'+add][:]  # fpdim
                        varDict_fpdim['SurfPrecipTotRate'] =  nc.variables['SurfPrecipTotRate'+add][:]  # fpdim
        #            varDict_fpdim['heightStormTop'] = nc.variables['heightStormTop'][:]   # fpdim
                        varDict_fpdim['heightStormTop'] = (nc.variables['heightStormTop'+add][:] / 1000.0) - site_elev  # fpdim
                    varDict_fpdim['scanNum'] =  nc.variables['scanNum'+add][:]  # fpdim
                    varDict_fpdim['rayNum'] =  nc.variables['rayNum'+add][:]  # fpdim
        #            varDict_fpdim['BBheight'] =  nc.variables['BBheight'][:]  # fpdim
                    # convert MSL to AGL

                    VN_filename = os.path.basename(filename)
                    # extract GPM orbit number from VN filename
                    orbit_number = int(VN_filename.split('.')[3])
                    #print("orbit number: ", orbit_number)

                    GPM_SENSOR=''
                    if data_is_dprgmi:
                        GPM_SENSOR = 'DPRGMI'
                    else:
                        DPR_2ADPR_file = getattr(nc, 'DPR_2ADPR_file')
                        DPR_2AKU_file = getattr(nc, 'DPR_2AKU_file')
                        DPR_2AKA_file = getattr(nc, 'DPR_2AKA_file')
                        # DPR_2BCMB_file = getattr(nc, 'DPR_2BCMB_file')
                        if not DPR_2ADPR_file.startswith('no_'):
                            GPM_SENSOR = 'DPR'
                        elif not DPR_2AKU_file.startswith('no_'):
                            GPM_SENSOR = 'Ku'
                        elif not DPR_2AKA_file.startswith('no_'):
                            GPM_SENSOR = 'Ka'
                        # elif not DPR_2BCMB_file.startswith('no_'):
                        #     GPM_SENSOR = 'DPRGMI'
                        else:
                            GPM_SENSOR = 'Unknown'
                    GR_site = getattr(nc, 'GR_file').split('_')[0]

                    if data_is_dprgmi:
                        zero_altitude = nc.variables['zeroDegAltitude'+add][:] / 1000.0

                    if GR_site in alt_bright_band.keys() and orbit_number in alt_bright_band[GR_site].keys():
                        alt_BB_height = alt_bright_band[GR_site][orbit_number]
                    else:
                        alt_BB_height = -9999.0

                    if data_is_dprgmi:
                        varDict_fpdim['BBheight'] =  np.empty_like(varDict_fpdim['piaFinal'])
                        varDict_fpdim['BBheight'][:] = -9999.0
                        varDict_fpdim['BBstatus'] =  np.empty_like(varDict_fpdim['piaFinal'])
                        varDict_fpdim['BBstatus'][:] =  -9999.0
                        #varDict_fpdim['TypePrecip'] =  nc.variables['precipitationType'+add][:]/10000000  # fpdim
                        # varDict_fpdim['precipTotWaterCont'] =  nc.variables['precipTotWaterCont'+add][:]  # fpdim
                    else:
                        varDict_fpdim['BBheight'] =  (nc.variables['BBheight'][:] / 1000.0) - site_elev  # fpdim
                        varDict_fpdim['BBstatus'] =  nc.variables['BBstatus'][:]  # fpdim
                        #varDict_fpdim['TypePrecip'] =  nc.variables['TypePrecip'][:]  # fpdim
                        # varDict_fpdim['precipTotWaterCont'] =  np.empty_like(varDict_fpdim['scanNum'])
                        # varDict_fpdim['precipTotWaterCont'][:] =  -9999.0

                    precip_rate_thresh = nc.variables['rain_min'][...]
                    #print("rain_min ", precip_rate_thresh)
                    dbz_thresh = nc.variables['DPR_dBZ_min'][...]
                    #print("dbz_min ", dbz_thresh)

                    # Elevation angle for surface radar, handled differently
                    elevationAngle =  nc.variables['elevationAngle'][:]  # elevdim
        #            elevationAngle =  index of elev loop

                    # time of GPM closest approach to radar site, handled differently
                    closestTime = str(chartostring(nc.variables['atimeNearestApproach'][:], encoding='utf-8'))
                    year = closestTime.split('-')[0]
                    month = closestTime.split('-')[1]
                    day = closestTime.split('-')[2].split(' ')[0]
                    # extract out only time field
                    closestTime = closestTime.split(' ')[1]

                    #print('attribs ', nc.ncattrs())
                    GPM_VERSION = getattr(nc, 'DPR_Version')
                    if data_is_dprgmi:
                        SCAN_TYPE = add.strip('_')
                    else:
                        SCAN_TYPE = getattr(nc, 'DPR_ScanType')
                    vn_version =  str(ma.getdata(nc.variables['version'][0]))
                    #print('version ' + str(vn_version))


                    # pick variable with most dimensions to define loops
                    elevations = nc.variables['GR_HID'+add].shape[0]
                    fpdim = nc.variables['GR_HID'+add].shape[1]
                    hidim = nc.variables['GR_HID'+add].shape[2]

                    count=0
                    site_rainy_count = 0
                    for fp in range(fpdim):
                        for elev in range(elevations):
                            #if varDict_elev_fpdim['PrecipRate'][elev][fp] >= precip_rate_thresh:
                            if varDict_elev_fpdim['PrecipRate'][elev][fp] >= precip_rate_thresh or varDict_elev_fpdim['GR_RC_rainrate'][elev][fp] >= precip_rate_thresh:
                                site_rainy_count = site_rainy_count + 1
                                break
                    #print("rainy count ", site_rainy_count)
                    #print("fpdim ", fpdim)
                    #percent_rainy = float(site_rainy_count)/float(fpdim)
                    percent_rainy = 100.0 * float(site_rainy_count)/float(fpdim)
                    #print ("percent rainy ", percent_rainy)

                    # compute mean BB
                    if data_is_dprgmi:
                        meanBB = compute_mean_BB_DPRGMI(zero_altitude)
                    else:
                        meanBB = compute_mean_BB(varDict_fpdim['BBheight'], varDict_fpdim['BBstatus'],nc.variables['TypePrecip'])
                    if meanBB < 0.0:
                        if alt_BB_height > 0.0:
                            meanBB = alt_BB_height
                            print("missing Bright band, using Ruc_0 height ", meanBB)
                        else:
                            meanBB = -9999.0
                            print("missing Bright band and Ruc_0 height...")
                    # else:
                    #     print("Mean Bright band ", meanBB)

                    for elev in range(elevations):
                        for fp in range(fpdim):
                            # only use matchup volumes > minimimum rain rate
                            if varDict_elev_fpdim['PrecipRate'][elev][fp] < precip_rate_thresh and varDict_elev_fpdim['GR_RC_rainrate'][elev][fp] < precip_rate_thresh:
                                continue
                            # if varDict_elev_fpdim['PrecipRate'][elev][fp] < precip_rate_thresh:
                            #     continue
                            #fp_entry={}
                            # put in non varying and metadata values for VN file
                            # fp_entry={"GPM_ver": GPM_VERSION, "VN_ver": vn_version, "scan": SCAN_TYPE, "sensor": GPM_SENSOR,
                            #           "GR_site": GR_site,"time": closestTime, "elev":float(elevationAngle[elev]),
                            #           "vn_filename":VN_filename, "site_percent_rainy":percent_rainy,
                            #           "site_rainy_count":site_rainy_count, "site_fp_count":fpdim,
                            #           "site_elev":site_elev, "meanBB":meanBB}
                            fp_entry={"time": closestTime, "elev":float(elevationAngle[elev]),
                                      "vn_filename":VN_filename, "site_percent_rainy":percent_rainy,
                                      "site_rainy_count":site_rainy_count, "site_fp_count":fpdim,
                                      "site_elev":site_elev, "meanBB":meanBB}
                            if data_is_dprgmi:
                                fp_entry["ruc_0_height"] = float(ma.getdata(zero_altitude)[fp])
                            else:
                                fp_entry["ruc_0_height"] = alt_BB_height

                            for fp_key in varDict_fpdim:
                                #print("fp_key ",fp_key)
                                # override specific int values in the dictionary
                                if fp_key=='BBstatus' or fp_key=='scanNum' or fp_key=='rayNum':
                                    fp_entry[fp_key] = int(ma.getdata(varDict_fpdim[fp_key])[fp])
                                else:
                                    fp_entry[fp_key]=float(ma.getdata(varDict_fpdim[fp_key])[fp])
                            for fp_elev_key in varDict_elev_fpdim:
                                #print("fp_elev_key ",fp_elev_key)
                                fp_entry[fp_elev_key] = float(ma.getdata(varDict_elev_fpdim[fp_elev_key][elev])[fp])
                            for id in range(hidim):
                                fp_entry["hid_"+str(id+1)] = int(ma.getdata(hid[elev][fp])[id])
                            if have_blockage == 1:
                                fp_entry['GR_blockage'] = float(ma.getdata(nc.variables['GR_blockage'+add][elev])[fp])
                            else:
                                fp_entry['GR_blockage'] = -9999.0

                            # compute BB proximity
                            #            varDict_elev_fpdim['topHeight'] = nc.variables['topHeight'][:] # elevAngle 	fpdim
                            #               varDict_elev_fpdim['bottomHeight'] = nc.variables['bottomHeight'][:] # elevAngle 	fpdim

                            bbprox = compute_BB_prox(meanBB, float(ma.getdata(varDict_elev_fpdim['topHeight'][elev])[fp]),
                                                     float(ma.getdata(varDict_elev_fpdim['bottomHeight'][elev])[fp]))
                            #fp_entry['BBprox'] = bbprox

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
                            gr_exp = float(ma.getdata(nc.variables['n_gr_expected'+add][elev])[fp])
                            gr_rej = float(ma.getdata(nc.variables['n_gr_z_rejected'+add][elev])[fp])
                            #print ("gr_exp ", gr_exp, " gr_rej ",gr_rej)
                            if len(ma.getdata(nc.variables['n_dpr_expected'+add].shape)) == 3:
                                dpr_exp = float((ma.getdata(nc.variables['n_dpr_expected' + add][elev])[fp])[0])
                            else:
                                dpr_exp = float(ma.getdata(nc.variables['n_dpr_expected'+add][elev])[fp])
                            if data_is_dprgmi:
                                if len(ma.getdata(nc.variables['n_dpr_expected' + add].shape)) == 3:
                                    dpr_rej = float(
                                        (ma.getdata(nc.variables['n_correctedReflectFactor_rejected' + add][elev])[fp])[0])
                                else:
                                    dpr_rej = float(ma.getdata(nc.variables['n_correctedReflectFactor_rejected'+add][elev])[fp])
                                type_precip = int(ma.getdata(nc.variables['precipitationType'+add][fp]) / 10000000)
                            else:
                                dpr_rej = float(ma.getdata(nc.variables['n_dpr_corr_z_rejected'][elev])[fp])
                                type_precip = int(ma.getdata(nc.variables['TypePrecip'][fp]))

                            #print ("dpr_exp ", dpr_exp, " dpr_rej ",dpr_rej)
                            if gr_exp > 0:
                                fp_entry['GR_beam'] = 100.0 * (gr_exp - gr_rej)/gr_exp
                            else:
                                fp_entry['GR_beam'] = 0.0
                            if dpr_exp > 0:
                                fp_entry['DPR_beam'] = 100.0 * (dpr_exp - dpr_rej)/dpr_exp
                            else:
                                fp_entry['DPR_beam'] = 0.0
                            # put last in record for partitioning
                            fp_entry['GPM_ver']=GPM_VERSION
                            fp_entry['VN_ver']=vn_version
                            fp_entry['sensor']=GPM_SENSOR
                            fp_entry['scan']=SCAN_TYPE
                            fp_entry['year']=year
                            fp_entry['month']=month
                            fp_entry['day']=day
                            fp_entry['GR_site']=GR_site
                            fp_entry['TypePrecip']=type_precip
                            fp_entry['BBprox'] = bbprox

                            #print(fp_entry)
                            #exit(0)
                            outputJson.append(fp_entry)
                            count = count + 1
    except Exception as err:
        print('process_file: error occurred: ', err)
        return {'error': 'Error occurred during process_file'}

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

    # local_directory = '/data/capri_test_data/VN/2019/'
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

#    VN_DIR = '/media/sf_berendes/capri_test_data/VN/mrms_geomatch'

    VN_DIR = '/data/capri_test_data/VN/wget/GPM/2BDPRGMI/V06A/1_3'
#    VN_DIR = '/data/capri_test_data/VN/wget/GPM/2AKu/NS/V06A/1_21'
#    VN_DIR = '/data/capri_test_data/VN/wget/GPM/2ADPR/MS/V06A/1_21'
#    VN_DIR = '/data/capri_test_data/VN/wget/GPM/2ADPR/HS/V06A/1_21'
#    VN_DIR = '/data/capri_test_data/VN/wget/GPM/2ADPR/NS/V06A/1_21'
#    VN_DIR = '/data/capri_test_data/VN/wget/GPM/2ADPR/MS/V06A/1_21'
#    VN_DIR = '/data/capri_test_data/VN/wget/GPM/2ADPR/HS/V06A/1_21'

    # OUT_DIR = '/data/capri_test_data/VN_parquet_dprgmi'
    # META_DIR = '/data/capri_test_data/meta_dprgmi'
    # alt_bb_file = '/data/capri_test_data/BB/GPM_rain_event_bb_km.txt'
    # s3_bucket = 'capri-data'
    # s3_dir = 'parquet_dprgmi'
    # meta_dir = 'metadata_dprgmi'
    # site_pattern = 'K'
    # upload_bin = False
    # upload_img = False
    # process_parquet_meta = True
    # upload_parquet = False
    # upload_meta = False
    # overwrite_upload_flag = True
    # save_json = True
    # reprocess_flag = False # don't reprocess if output parquet already exists

    config = {
        "VN_DIR": "/data/capri_test_data/VN/wget/GPM/2BDPRGMI/V06A/1_3",
        "OUT_DIR": "/data/capri_test_data/VN_parquet_dprgmi",
        "META_DIR": "/data/capri_test_data/meta_dprgmi",
        "alt_bb_file": "/data/capri_test_data/BB/GPM_rain_event_bb_km.txt",
        "s3_bucket": "capri-data",
        "s3_parquet_dir": "parquet_dprgmi",
        "s3_img_dir": "img",
        "s3_bin_dir": "bin",
        "s3_meta_dir": "metadata_dprgmi",
        "site_pattern": "K",
        "upload_bin": False,
        "upload_img": False,
        "process_parquet_meta": True,
        "upload_parquet": False,
        "upload_meta": False,
        "overwrite_upload_flag": True,
        "save_json": True,
        "reprocess_flag": False
    }
    #config_file = "run_dprgmi.json"

    if len(sys.argv)>1:
        config_file = sys.argv[1]

    try:
        with open(config_file) as f:
            config = json.load(f)
    except Exception as err:
        print('Error opening file ', config_file, " - ", err)
        sys.exit(-1)

    if len(sys.argv)>2:
        config['VN_DIR'] = sys.argv[2]

    #client = boto3.client('s3')
    bright_band = read_alt_bb_file(config['alt_bb_file'])

    for root, dirs, files in os.walk(config['VN_DIR'], topdown=False):
        for file in files:
            #print('file ' + file)
            # only process zipped nc VN files
            if len(config['site_pattern'])>0:
                if file.split('.')[1].startswith(config['site_pattern']):
                    do_file=True
                else:
                    do_file = False
            else:
                do_file = True
            if file.endswith('.nc.gz') and do_file:
                print('processing file: ' + file)
                if config['process_parquet_meta']:
                    parquet_output_file = os.path.join(config['OUT_DIR'],file+'.parquet')
                    json_output_file = os.path.join(config['OUT_DIR'],file+'.json.gz')
                    if os.path.isfile(parquet_output_file):
                        if not config['reprocess_flag']:
                            print("file ",parquet_output_file," already exists, skipping...")
                            continue
                        else:
                            print("file ",parquet_output_file," already exists, reprocessing...")

                    outputJson = process_file(os.path.join(root,file), bright_band)
                    # no precip volumes were found, skip file
                    if len(outputJson) == 0:
                        print("found no precip in file " + file + " skipping...")
                        continue
                    #print(outputJson)
                    if 'error' in outputJson:
                        print('skipping file ', file, ' due to processing error...')
                        continue
                    #print (outputJson)
                    parquet_data = json2parquet.ingest_data(outputJson)
                    os.makedirs(os.path.join(config['OUT_DIR']), exist_ok=True)
                    json2parquet.write_parquet(parquet_data, parquet_output_file, compression='snappy')

                    if config['save_json']:
                        with gzip.open(json_output_file, 'wt', encoding="ascii") as zipfile:
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
                    os.makedirs(os.path.join(config['META_DIR']), exist_ok=True)
                    with open(os.path.join(config['META_DIR'],file+'.meta.json'), 'w') as json_file:
                        json.dump(metadata, json_file)
                    json_file.close()

                    if config['upload_parquet']:
                        #print("uploading parquet "+os.path.join(OUT_DIR,file+'.parquet'))
                        parquet_key = config['s3_parquet_dir']+'/'+file+'.parquet'
                        upload_s3(os.path.join(config['OUT_DIR'],file+'.parquet',), config['s3_bucket'], parquet_key,config['overwrite_upload_flag'])

                    if config['upload_meta']:
                        #print("uploading metadata "+os.path.join(META_DIR,file+'.meta.json'))
                        metadata_key = config['s3_meta_dir']+'/'+file+'.meta.json'
                        upload_s3(os.path.join(config['META_DIR'],file+'.meta.json'), config['s3_bucket'], metadata_key,config['overwrite_upload_flag'])

                # look for deep leraning training and image files with same base filename
                # put deep learning binary files and images in S3
                if config['upload_bin']:
                    # check for GPM and MRMS DL training files (.bin)
                    if os.path.isfile(os.path.join(root, file + '.gpm.bin')):
                        upload_s3(os.path.join(root,file+'.gpm.bin'), config['s3_bucket'], config['s3_bin_dir']+'/'+file+'.gpm.bin',config['overwrite_upload_flag'])
                    if os.path.isfile(os.path.join(root, file + '.mrms.bin')):
                        upload_s3(os.path.join(root,file+'.mrms.bin'), config['s3_bucket'], config['s3_bin_dir']+'/'+file+'.mrms.bin',config['overwrite_upload_flag'])

                if config['upload_img']:
                    # check for GPM and MRMS DL images and kml files
                    # if os.path.isfile(os.path.join(root, file + '.gpm.bw.png')):
                    #     upload_s3(os.path.join(root,file+'.gpm.bw.png'), s3_bucket, config['s3_img_dir']+'/'+file+'.gpm.bw.png',overwrite_upload_flag)
                    # if os.path.isfile(os.path.join(root, file + '.gpm.bw.kml')):
                    #     upload_s3(os.path.join(root,file+'.gpm.bw.kml'), s3_bucket, config['s3_img_dir']+'/'+file+'.gpm.bw.kml',overwrite_upload_flag)
                    if os.path.isfile(os.path.join(root, file + '.gpm.col.png')):
                        upload_s3(os.path.join(root,file+'.gpm.col.png'), config['s3_bucket'], config['s3_img_dir']+'/'+file+'.gpm.col.png',config['overwrite_upload_flag'])
                    if os.path.isfile(os.path.join(root, file + '.gpm.col.kml')):
                        upload_s3(os.path.join(root,file+'.gpm.col.kml'), config['s3_bucket'], config['s3_img_dir']+'/'+file+'.gpm.col.kml',config['overwrite_upload_flag'])

                    # if os.path.isfile(os.path.join(root, file + '.mrms.bw.png')):
                    #     upload_s3(os.path.join(root,file+'.mrms.bw.png'), s3_bucket, config['s3_img_dir']+'/'+file+'.mrms.bw.png',overwrite_upload_flag)
                    # if os.path.isfile(os.path.join(root, file + '.mrms.bw.kml')):
                    #     upload_s3(os.path.join(root,file+'.mrms.bw.kml'), s3_bucket, config['s3_img_dir']+'/'+file+'.mrms.bw.kml',overwrite_upload_flag)
                    if os.path.isfile(os.path.join(root, file + '.mrms.col.png')):
                        upload_s3(os.path.join(root,file+'.mrms.col.png'), config['s3_bucket'], config['s3_img_dir']+'/'+file+'.mrms.col.png',config['overwrite_upload_flag'])
                    if os.path.isfile(os.path.join(root, file + '.mrms.col.kml')):
                        upload_s3(os.path.join(root,file+'.mrms.col.kml'), config['s3_bucket'], config['s3_img_dir']+'/'+file+'.mrms.col.kml',config['overwrite_upload_flag'])

                #sys.exit()


    #outputJson = process_file("/data/capri_test_data/VN/2019/GRtoDPR.KEOX.190807.30913.V06A.DPR.NS.1_21.nc.gz")

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
