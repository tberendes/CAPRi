# ---------------------------------------------------------------------------------------------
#
#  extract_vn.py
#
#  Description: Extracts data from an GPM VN file, formatted in netCDF
#               and parses out the values and formats output for JSON
#
#  Syntax: currently no input parameters
#
#  To Do: modify to accept input parameters, such as filename and maybe the location of district coordinates
#
# ---------------------------------------------------------------------------------------------
import gzip
import pickle
from io import BytesIO
from netCDF4 import Dataset as NetCDFFile
import numpy as np
from numpy import ma
import os
from netCDF4 import chartostring
import csv

def read_alt_bb_file(filename):
    alt_bb_dict = {}
    # check to see if .pcl is in filename, assume pickle file is passed as filename
    if str(filename).endswith('.pcl'):
        read_from_csv=False
    else:
        read_from_csv=True

    if not read_from_csv:
        print("reading pickled BB file " + filename)
        f = open(filename, "rb")
        alt_bb_dict = pickle.load(f)
        f.close()
    else:
        # read csv (delimited with '|')
        print("reading CSV BB file " + filename)
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


def zip_string(str_data: str) -> bytes:
    btsio = BytesIO()
    g = gzip.GzipFile(fileobj=btsio, mode='w')
    g.write(bytes(str_data, 'utf8'))
    g.close()
    return btsio.getvalue()

def compute_mean_height(height):
    fpdim = len(height)
    mean = 0.0
    count = 0
    for fp in range(fpdim):
        if height[fp] > 0:
            mean = mean + height[fp]
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
                have_mrms = False
                if filename.find('DPRGMI')>=0:
                    data_is_dprgmi = True
                    addlist=['_NS','_MS']
                else:
                    data_is_dprgmi = False
                    addlist=['']
                if filename.find('GRtoDPR.')>=0:
                    NW_ScaleFactor = 10.0
                else:
                    NW_ScaleFactor = 1.0

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
#                        varDict_elev_fpdim['Nw'] =  nc.variables['Nw'+add][:]  # elevAngle 	fpdim
                        # added NW_ScaleFactor for DPR to fix log scaling
                        varDict_elev_fpdim['Nw'] =  nc.variables['Nw'+add][:] / NW_ScaleFactor # elevAngle 	fpdim
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
                        # TODO: currently PrecipMeanHigh is only generated for DPR files, will need to add DPRGMI when available
                        varDict_fpdim['MRMSPrecip'] = np.empty_like(varDict_fpdim['piaFinal'])
                        varDict_fpdim['MRMSPrecip'][:] = -9999.0

                    else:
                        varDict_fpdim['piaFinal'] = nc.variables['piaFinal'][:]  # fpdim
                        varDict_fpdim['PrecipRateSurface'] =  nc.variables['PrecipRateSurface'+add][:]  # fpdim
                        varDict_fpdim['SurfPrecipTotRate'] =  nc.variables['SurfPrecipTotRate'+add][:]  # fpdim
        #            varDict_fpdim['heightStormTop'] = nc.variables['heightStormTop'][:]   # fpdim
                        varDict_fpdim['heightStormTop'] = (nc.variables['heightStormTop'+add][:] / 1000.0) - site_elev  # fpdim

                        if 'PrecipMeanHigh' in nc.variables.keys():
                            varDict_fpdim['MRMSPrecip'] = nc.variables['PrecipMeanHigh']
                            have_mrms = True
                        else: # MRMS not present in DPR file
                            varDict_fpdim['MRMSPrecip'] = np.empty_like(varDict_fpdim['piaFinal'])
                            varDict_fpdim['MRMSPrecip'][:] = -9999.0

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
                        zero_altitude = (nc.variables['zeroDegAltitude'+add][:] / 1000.0) - site_elev
                    else: # added zero_altitude for DPR, Ku, Ka
                        if 'heightZeroDeg' in nc.variables.keys():
                            zero_altitude = (nc.variables['heightZeroDeg'][:] / 1000.0) - site_elev
                        else:
                            zero_altitude = np.empty_like(varDict_fpdim['piaFinal'])
                            zero_altitude[:] = -9999.0

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
                    year = int(closestTime.split('-')[0])
                    month = int(closestTime.split('-')[1])
                    day = int(closestTime.split('-')[2].split(' ')[0])
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

                    mean_zeroDeg = compute_mean_height(zero_altitude)
                    # compute mean BB
                    if data_is_dprgmi:
                        meanBB = mean_zeroDeg
                    else:
                        meanBB = compute_mean_BB(varDict_fpdim['BBheight'], varDict_fpdim['BBstatus'],nc.variables['TypePrecip'])
                    if meanBB < 0.0:
                        if mean_zeroDeg > 0.0:
                            meanBB = mean_zeroDeg
                        else:
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
                                      "site_elev":site_elev, "meanBB":meanBB, "fp":fp}
                            #if data_is_dprgmi:
                            #    fp_entry["ruc_0_height"] = float(ma.getdata(zero_altitude)[fp])
                            #else:
                            #    fp_entry["ruc_0_height"] = alt_BB_height
                            if mean_zeroDeg > 0.0: # we have a mean value so there are some valid individual values
                                fp_entry["zero_deg_height"] = float(ma.getdata(zero_altitude)[fp])
                            else:
                                fp_entry["zero_deg_height"] = alt_BB_height

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

    return have_mrms, outputJson
