# ---------------------------------------------------------------------------------------------
#
#  extract_vn.py
#
#  Description: Extracts data from a GPM VN file, formatted in netCDF
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
    #bbwidth=0.750
    bbwidth=750 # assume 1.5 km layer width centered on BB height
    htcat = -9999
    if meanBB > 0.0:
        if botm > (meanBB+bbwidth):
            htcat = 3 # above BB
        if top > 0.0 and top < (meanBB-bbwidth):
            htcat = 1 # below BB
        if botm <= (meanBB+bbwidth) and top >= (meanBB-bbwidth):
            htcat = 2  # within BB
    return htcat
def compute_liquid_hid_flag(bottom_height, freezing_level_height, gr_hid):
    # Instead of simply using the bright band or freezing level height to partition the VN data
    # going into Athena, here’s some pseudo-code that uses the HID:
    # Initialize 2D flag with fpdim, elev (i.e., a matched volume)
    # Make sure matched volume is below freezing level height
    # Flag matched volume if it only contains liquid HID types (DZ, RA, BD)

    #                liquid_hid_flag=intarr(fpdim,elev)*0.0 ;initialize 2-D flag to indicate hydrometeor type as liquid (1) or non-liquid (0)
    # For i=0, fpdim-1 do $
    # if ( bottomHeight[i] LT (freezing_level[i]-1000 meters) ) do begin ;forget if heights are meters or kilometers
    # rind =where( total(GR_HID [i,*,0]+GR_HID[i,*,3:9]+GR_HID[I,*,11],  0) EQ 0, rcount) ;sum rain/dz/bd hid bins
    # if ( rcount gt 0) liquid_hid_flag[fpdim,elev]=1
    # endif
    liquid_hid_flag = False
#    if (freezing_level_height<0 or bottom_height <0):
#        liquid_hid_flag = False
    if (freezing_level_height >= 0 and bottom_height >= 0 and bottom_height < (freezing_level_height-1000 )):
            # sum hid counts that are not liquid (including missing)
            #cnt = int(gr_hid[0])+int(gr_hid[3]) +int(gr_hid[4]) +int(gr_hid[5]) +int(gr_hid[6]) +int(gr_hid[7]) +int(gr_hid[8]) +int(gr_hid[9]) +int(gr_hid[11])
            cnt = int(gr_hid[0])+int(gr_hid[3]) +int(gr_hid[4]) +int(gr_hid[5]) +int(gr_hid[6]) +int(gr_hid[7]) +int(gr_hid[8]) +int(gr_hid[9])
            if cnt > 0:
                liquid_hid_flag = True

    return liquid_hid_flag

def process_file(filename):
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

                GPM_VERSION = getattr(nc, 'DPR_Version')

                # check for ITE and V, and extract numeric version
                if GPM_VERSION.find('ITE')>=0:
                    versionNumber=int(GPM_VERSION.split('ITE')[1])
                    if versionNumber < 600:
                        isVersion7 = False
                    else:
                        isVersion7 = True
                else: #GPM_VERSION.find('V')>=0:
                    versionNumber = int(GPM_VERSION[1:3])
                    if versionNumber <= 6:
                        isVersion7 = False
                    else:
                        isVersion7 = True

                addlist=[]
                have_mrms = False
                if filename.find('DPRGMI')>=0:
                    data_is_dprgmi = True
                    if isVersion7:
                        addlist=['_NS','_FS']
                    else:
                        addlist = ['_NS', '_MS']
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
                        #varDict_elev_fpdim['ZFactorCorrected'] = float(ma.getdata(nc.variables['GR_blockage' + add][elev])[fp])
                        shape = nc.variables['correctedReflectFactor'+add].shape
                        eld = shape[0]
                        fpd = shape[1]
                        if len(shape) == 2:
                            varDict_elev_fpdim['correctedReflectFactor'] = nc.variables['correctedReflectFactor'+add][:]  # elevAngle 	fpdim
                        else:  # MS swath has extra dimension, use only 1st dimension (ku) value
                            #nkukad = nc.variables['correctedReflectFactor'+add].shape[2]
                            varDict_elev_fpdim['correctedReflectFactor'] = np.empty_like(varDict_elev_fpdim['GR_Z']) # elevAngle 	fpdim
                            for el in range(eld):
                                for fp in range(fpd):
                                    # using Ku version, Ku [0], Ka[1]
                                    varDict_elev_fpdim['correctedReflectFactor'][el][fp] =  float(ma.getdata(nc.variables['correctedReflectFactor'+add][el])[fp][0])
                    else:
                        varDict_elev_fpdim['ZFactorMeasured'] = nc.variables['ZFactorMeasured'][:]  # elevAngle 	fpdim
                        varDict_elev_fpdim['airTemperature'] = nc.variables['airTemperature'][:]  # elevAngle 	fpdim
                        if isVersion7:
                            varDict_elev_fpdim['ZFactorFinal'] = nc.variables['ZFactorFinal'][:]  # elevAngle 	fpdim
                        else:
                            varDict_elev_fpdim['ZFactorCorrected'] =  nc.variables['ZFactorCorrected'][:] # elevAngle 	fpdim
                    if data_is_dprgmi:
                        varDict_elev_fpdim['precipTotRate'] = nc.variables['precipTotRate' + add][:]  # elevAngle 	fpdim
                    else:
                        varDict_elev_fpdim['PrecipRate'] = nc.variables['PrecipRate'][:]   # elevAngle 	fpdim

                    varDict_elev_fpdim['GR_RC_rainrate'] = nc.variables['GR_RC_rainrate'+add][:]  # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RC_rainrate_StdDev'] = nc.variables['GR_RC_rainrate_StdDev'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RC_rainrate_Max'] =  nc.variables['GR_RC_rainrate_Max'+add][:]  # elevAngle 	fpdim
#                    varDict_elev_fpdim['GR_RR_rainrate'] = nc.variables['GR_RR_rainrate'+add][:]   # elevAngle 	fpdim
#                    varDict_elev_fpdim['GR_RR_rainrate_StdDev'] =  nc.variables['GR_RR_rainrate_StdDev'+add][:]  # elevAngle 	fpdim
#                    varDict_elev_fpdim['GR_RR_rainrate_Max'] = nc.variables['GR_RR_rainrate_Max'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RP_rainrate'] = nc.variables['GR_RP_rainrate'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RP_rainrate_StdDev'] =  nc.variables['GR_RP_rainrate_StdDev'+add][:]  # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RP_rainrate_Max'] = nc.variables['GR_RP_rainrate_Max'+add][:]   # elevAngle 	fpdim

                    varDict_elev_fpdim['GR_Dm'] = nc.variables['GR_Dm'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Dm_StdDev'] =  nc.variables['GR_Dm_StdDev'+add][:]  # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Dm_Max'] = nc.variables['GR_Dm_Max'+add][:]   # elevAngle 	fpdim

#                    varDict_elev_fpdim['GR_sigmaDm'] = nc.variables['GR_sigmaDm'+add][:]   # elevAngle 	fpdim
#                    varDict_elev_fpdim['GR_sigmaDm_StdDev'] =  nc.variables['GR_sigmaDm_StdDev'+add][:]  # elevAngle 	fpdim
#                    varDict_elev_fpdim['GR_sigmaDm_Max'] = nc.variables['GR_sigmaDm_Max'+add][:]   # elevAngle 	fpdim
                    if data_is_dprgmi:
                        if isVersion7:
                            varDict_elev_fpdim['precipTotDm'] = nc.variables['precipTotDm' + add][:]  # elevAngle 	fpdim
                        else:
                            varDict_elev_fpdim['precipTotPSDparamHigh'] = nc.variables['precipTotPSDparamHigh'+add][:]   # elevAngle 	fpdim
                    else:
                        varDict_elev_fpdim['Dm'] = nc.variables['Dm'][:]  # elevAngle 	fpdim

                    varDict_elev_fpdim['GR_Zdr'] =  nc.variables['GR_Zdr'+add][:]  # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_RHOhv'] = nc.variables['GR_RHOhv'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Nw'] =  nc.variables['GR_Nw'+add][:]  # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Nw_StdDev'] = nc.variables['GR_Nw_StdDev'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_Nw_Max'] = nc.variables['GR_Nw_Max'+add][:]   # elevAngle 	fpdim

                    #if isVersion7:
                    varDict_elev_fpdim['GR_liquidWaterContent'] = nc.variables['GR_liquidWaterContent'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_liquidWaterContent_StdDev'] = nc.variables['GR_liquidWaterContent_StdDev'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_liquidWaterContent_Max'] = nc.variables['GR_liquidWaterContent_Max'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_frozenWaterContent'] = nc.variables['GR_frozenWaterContent'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_frozenWaterContent_StdDev'] = nc.variables['GR_frozenWaterContent_StdDev'+add][:]   # elevAngle 	fpdim
                    varDict_elev_fpdim['GR_frozenWaterContent_Max'] = nc.variables['GR_frozenWaterContent_Max'+add][:]   # elevAngle 	fpdim
                    # else:
                    #     varDict_elev_fpdim['GR_liquidWaterContent'] = np.empty_like(varDict_elev_fpdim['GR_Nw'])  # elevAngle 	fpdim
                    #     varDict_elev_fpdim['GR_liquidWaterContent'][:] =  -9999.0
                    #     varDict_elev_fpdim['GR_frozenWaterContent'] = np.empty_like(varDict_elev_fpdim['GR_Nw'])   # elevAngle 	fpdim
                    #     varDict_elev_fpdim['GR_frozenWaterContent'][:] =  -9999.0
                    #     varDict_elev_fpdim['GR_liquidWaterContent_StdDev'] = np.empty_like(varDict_elev_fpdim['GR_Nw'])  # elevAngle 	fpdim
                    #     varDict_elev_fpdim['GR_liquidWaterContent_StdDev'][:] =  -9999.0
                    #     varDict_elev_fpdim['GR_frozenWaterContent_StdDev'] = np.empty_like(varDict_elev_fpdim['GR_Nw'])   # elevAngle 	fpdim
                    #     varDict_elev_fpdim['GR_frozenWaterContent_StdDev'][:] =  -9999.0
                    #     varDict_elev_fpdim['GR_liquidWaterContent_Max'] = np.empty_like(varDict_elev_fpdim['GR_Nw'])  # elevAngle 	fpdim
                    #     varDict_elev_fpdim['GR_liquidWaterContent_Max'][:] =  -9999.0
                    #     varDict_elev_fpdim['GR_frozenWaterContent_Max'] = np.empty_like(varDict_elev_fpdim['GR_Nw'])   # elevAngle 	fpdim
                    #     varDict_elev_fpdim['GR_frozenWaterContent_Max'][:] =  -9999.0

                    if data_is_dprgmi:
                        #varDict_elev_fpdim['Nw'] = nc.variables['precipTotPSDparamLow' + add][:,:,0:0]  # elevAngle 	fpdim
                        if isVersion7:
                            eld = nc.variables['precipTotLogNw' + add].shape[0]
                            fpd = nc.variables['precipTotLogNw' + add].shape[1]
                            varDict_elev_fpdim['precipTotLogNw'] = np.empty_like(varDict_elev_fpdim['GR_Nw'])  # elevAngle 	fpdim
                            nw_var = 'precipTotLogNw'
                        else:
                            eld = nc.variables['precipTotPSDparamLow' + add].shape[0]
                            fpd = nc.variables['precipTotPSDparamLow' + add].shape[1]
                            varDict_elev_fpdim['precipTotPSDparamLow'] = np.empty_like(varDict_elev_fpdim['GR_Nw'])  # elevAngle 	fpdim
                            nw_var = 'precipTotPSDparamLow'

                        #nPSDlo = nc.variables['precipTotPSDparamLow' + add].shape[2]

                        #varDict_elev_fpdim['Nw'] = np.empty_like(varDict_elev_fpdim['GR_Nw']) # elevAngle 	fpdim

                        clut_shape = nc.variables['clutterStatus' + add].shape
                        # handle kuka dimension for MS swath
                        if len(clut_shape)== 3:
                            varDict_elev_fpdim['clutterStatus'] = np.empty_like(nc.variables['n_gr_expected' + add])
                        else:
                            varDict_elev_fpdim['clutterStatus'] = nc.variables['clutterStatus' + add][:]
                        for el in range(eld):
                            for fp in range(fpd):
                                #varDict_elev_fpdim['Nw'][el][fp] =  float(ma.getdata(nc.variables['precipTotPSDparamLow' + add][el])[fp][0])
                                if isVersion7:
                                    mx = np.ma.masked_invalid(ma.getdata(nc.variables['precipTotLogNw' + add][el]))
                                    #if np.isnan(ma.getdata(nc.variables['precipTotLogNw' + add][el])[fp]):
                                    if not mx[fp]:
                                        val = -9999.0
                                    else:
                                        val = float(ma.getdata(nc.variables['precipTotLogNw' + add][el])[fp])
                                else:
                                    mx = np.ma.masked_invalid(ma.getdata(nc.variables['precipTotPSDparamLow' + add][el]))
                                    #if np.isnan(ma.getdata(nc.variables['precipTotPSDparamLow' + add][el])[fp]):
                                    if not mx[fp]:
                                        val = -9999.0
                                    else:
                                        val = float(ma.getdata(nc.variables['precipTotPSDparamLow' + add][el])[fp][0])
                                # scales log10(Nw) values from 1/m^4 to 1/m^3
                                # if val > 0:
                                #     varDict_elev_fpdim['Nw'][el][fp] = val - 3.0
                                # if len(clut_shape)== 3: # handle kuka dimension for MS swath (use ku)
                                #     varDict_elev_fpdim['clutterStatus'][el][fp] =  int(ma.getdata(nc.variables['clutterStatus' + add][el])[fp][0])
                                #if np.isnan(val):
                                if val > 0:
                                    varDict_elev_fpdim[nw_var][el][fp] = val - 3.0
                                    # if isVersion7:
                                    #     varDict_elev_fpdim['precipTotLogNw'][el][fp] = val - 3.0
                                    # else:
                                    #     varDict_elev_fpdim['precipTotPSDparamLow'][el][fp] = val - 3.0
                                else:
                                    varDict_elev_fpdim[nw_var][el][fp] = val
                                    # if isVersion7:
                                    #     varDict_elev_fpdim['precipTotLogNw'][el][fp] = val
                                    # else:
                                    #     varDict_elev_fpdim['precipTotPSDparamLow'][el][fp] = val


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

                    freezing_level_height = float(ma.getdata(nc.variables['freezing_level_height']).data)
                    if freezing_level_height >= 0:
                        freezing_level_height = freezing_level_height * 1000.0
                    else:
                        print("missing freezing_level_height in file ", filename)

                    #print("site_elev ", site_elev) # km
                    #exit(0)
        #            varDict_elev_fpdim['topHeight'] = 1000.0 * (nc.variables['topHeight'][:] + site_elev)  # elevAngle 	fpdim
        #            varDict_elev_fpdim['bottomHeight'] = 1000.0 * (nc.variables['bottomHeight'][:] + site_elev)  # elevAngle 	fpdim
                    #varDict_elev_fpdim['topHeight'] = nc.variables['topHeight'+add][:] # elevAngle 	fpdim
                    #varDict_elev_fpdim['bottomHeight'] = nc.variables['bottomHeight'+add][:] # elevAngle 	fpdim
                    varDict_elev_fpdim['topHeight'] = (nc.variables['topHeight'+add][:] + site_elev) * 1000.0 # elevAngle 	fpdim
                    varDict_elev_fpdim['bottomHeight'] = (nc.variables['bottomHeight'+add][:] + site_elev) * 1000.0 # elevAngle 	fpdim

                    if data_is_dprgmi:
                        varDict_elev_fpdim['precipTotWaterCont'] =  nc.variables['precipTotWaterCont'+add][:]  # fpdim
                    #else:
                        #varDict_elev_fpdim['precipTotWaterCont'] =  np.empty_like(varDict_elev_fpdim['topHeight'])
                        #varDict_elev_fpdim['precipTotWaterCont'][:] =  -9999.0

                    # Ground radar hydrometeor id histograms
                    hid = nc.variables['GR_HID'+add][:]   # elevAngle 	fpdim 	hidim

                    # variables indexed by fpdim (i.e. surface level or GPM footprint regardless of height
                    varDict_fpdim={}
                    if data_is_dprgmi:
                        shape = nc.variables['pia' + add].shape
                        fpd = shape[0]
                        if len(shape) == 1:
                            #varDict_fpdim['piaFinal'] = nc.variables['pia'+add][:]   # fpdim
                            varDict_fpdim['pia'] = nc.variables['pia'+add][:]   # fpdim
                        else:
                            nkukad = shape[1]
                            #varDict_fpdim['piaFinal'] = np.empty_like(nc.variables['DPRlatitude'+add]) # elevAngle 	fpdim
                            varDict_fpdim['pia'] = np.empty_like(nc.variables['DPRlatitude'+add]) # elevAngle 	fpdim
                            for fp in range(fpd):
                                # using Ku version, Ku [0], Ka[1]
                                #varDict_fpdim['piaFinal'][fp] =  float(ma.getdata(nc.variables['pia'+add])[fp][0])
                                varDict_fpdim['pia'][fp] =  float(ma.getdata(nc.variables['pia'+add])[fp][0])

                        #varDict_elev_fpdim['ZFactorMeasured'] = np.empty_like(varDict_elev_fpdim['GR_Z'])
                        #varDict_elev_fpdim['ZFactorMeasured'][:] = -9999.0

                        #varDict_fpdim['PrecipRateSurface'] =  np.empty_like(varDict_fpdim['piaFinal'])
                        #varDict_fpdim['PrecipRateSurface'][:] = -9999.0

                        varDict_fpdim['SurfPrecipTotRate'] =  nc.variables['surfPrecipTotRate'+add][:]  # fpdim

                        #varDict_fpdim['heightStormTop'] = np.empty_like(varDict_fpdim['piaFinal'])
                        #varDict_fpdim['heightStormTop'][:] = -9999.0

                        # TODO: currently PrecipMeanHigh is only generated for DPR files, will need to add DPRGMI when available
                        #varDict_fpdim['MRMSPrecip'] = np.empty_like(varDict_fpdim['piaFinal'])
                        #varDict_fpdim['MRMSPrecip'][:] = -9999.0

                    else:
                        varDict_fpdim['piaFinal'] = nc.variables['piaFinal'][:]  # fpdim
                        varDict_fpdim['PrecipRateSurface'] =  nc.variables['PrecipRateSurface'+add][:]  # fpdim
                        varDict_fpdim['SurfPrecipTotRate'] =  nc.variables['SurfPrecipTotRate'+add][:]  # fpdim
        #            varDict_fpdim['heightStormTop'] = nc.variables['heightStormTop'][:]   # fpdim
                        #varDict_fpdim['heightStormTop'] = (nc.variables['heightStormTop'+add][:] / 1000.0) - site_elev  # fpdim
                        varDict_fpdim['heightStormTop'] = nc.variables['heightStormTop'+add][:]  # fpdim

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

                    # count statistics from GR averaging
                    # varDict_elev_fpdim['n_gr_expected'] = nc.variables['n_gr_expected' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_z_rejected'] = nc.variables['n_gr_z_rejected' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_rc_rejected'] = nc.variables['n_gr_rc_rejected' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_rp_rejected'] = nc.variables['n_gr_rp_rejected' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_rr_rejected'] = nc.variables['n_gr_rr_rejected' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_nw_rejected'] = nc.variables['n_gr_nw_rejected' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_liquidWaterContent_rejected'] = nc.variables['n_gr_liquidWaterContent_rejected' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_frozenWaterContent_rejected'] = nc.variables['n_gr_frozenWaterContent_rejected' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_dm_rejected'] = nc.variables['n_gr_dm_rejected' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_sigmadm_rejected'] = nc.variables['n_gr_sigmadm_rejected' + add][:]  # fpdim
                    #
                    # varDict_elev_fpdim['n_gr_rc_precip'] = nc.variables['n_gr_rc_precip' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_rp_precip'] = nc.variables['n_gr_rp_precip' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_rr_precip'] = nc.variables['n_gr_rr_precip' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_nw_precip'] = nc.variables['n_gr_nw_precip' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_dm_precip'] = nc.variables['n_gr_dm_precip' + add][:]  # fpdim
                    # varDict_elev_fpdim['n_gr_sigmadm_precip'] = nc.variables['n_gr_sigmadm_precip' + add][:]  # fpdim

                    varDict_elev_fpdim['n_gr_liquidWaterContent_precip'] = nc.variables['n_gr_liquidWaterContent_precip' + add][:]  # fpdim
                    varDict_elev_fpdim['n_gr_frozenWaterContent_precip'] = nc.variables['n_gr_frozenWaterContent_precip' + add][:]  # fpdim

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

                    # use mean of fp-based zero degree heights if freezing_level_height is missing
                    # since the fp-based heights are also model-based
                    if freezing_level_height < 0:
                        if data_is_dprgmi:
                            zero_altitude = nc.variables['zeroDegAltitude' + add][:]
                        else:
                            zero_altitude = nc.variables['heightZeroDeg'][:]
                        freezing_level_height = compute_mean_height(zero_altitude)
                        if freezing_level_height < 0:
                            print('alternate freezing_level_height is missing')
                        else:
                            print('using alternate freezing_level_height ', freezing_level_height)

                    # if data_is_dprgmi:
                    #     #zero_altitude = (nc.variables['zeroDegAltitude'+add][:] / 1000.0) - site_elev
                    #     zero_altitude = nc.variables['zeroDegAltitude'+add][:]
                    # else: # added zero_altitude for DPR, Ku, Ka
                    #     if 'heightZeroDeg' in nc.variables.keys():
                    #         #zero_altitude = (nc.variables['heightZeroDeg'][:] / 1000.0) - site_elev
                    #         zero_altitude = nc.variables['heightZeroDeg'][:]
                    #     else:
                    #         zero_altitude = np.empty_like(varDict_fpdim['piaFinal'])
                    #         zero_altitude[:] = -9999.0

                    # Use new RUC-based freezing level variable in VN files for V7 VN version 2.2 (km)
                    #alt_BB_height = -9999.0
                    # if 'freezing_level_height' in nc.variables.keys():
                    #     #alt_BB_height = nc.variables['freezing_level_height']
                    #     alt_BB_height = float(ma.getdata(nc.variables['freezing_level_height']).data) * 1000.0
                    #     #alt_BB_height = nc.variables['freezing_level_height'] * 1000.0

                    #if GR_site in alt_bright_band.keys() and orbit_number in alt_bright_band[GR_site].keys():
                    #    alt_BB_height = alt_bright_band[GR_site][orbit_number]
                    #else:
                    #    alt_BB_height = -9999.0

                    # if data_is_dprgmi:
                    #     varDict_fpdim['BBheight'] =  np.empty_like(varDict_fpdim['pia'])
                    #     varDict_fpdim['BBheight'][:] = -9999.0
                    #     varDict_fpdim['BBstatus'] =  np.empty_like(varDict_fpdim['pia'])
                    #     varDict_fpdim['BBstatus'][:] =  -9999.0
                    #else:
                    if not data_is_dprgmi:
                        #varDict_fpdim['BBheight'] =  (nc.variables['BBheight'][:] / 1000.0) - site_elev  # fpdim
                        varDict_fpdim['BBheight'] =  nc.variables['BBheight'][:] # fpdim
                        varDict_fpdim['BBstatus'] =  nc.variables['BBstatus'][:]  # fpdim

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
#                    GPM_VERSION = getattr(nc, 'DPR_Version')
                    if data_is_dprgmi:
                        SCAN_TYPE = add.strip('_')
                    else:
                        SCAN_TYPE = getattr(nc, 'DPR_ScanType')
                    #vn_version =  str(ma.getdata(nc.variables['version'][0]))

                    #print('version ' + str(vn_version))
                    # new version 7 variables
                    # float precipWater(elevationAngle, fpdim);
                    #     long_name = "The amount of precipitable water";units = "g/m3";
                    # short flagInversion(fpdim=1245);
                    #     long_name = "TBD info for flagInversion";
                    #
                    # DPR FS scan ONLY
                    #
                    # Added the following new V7 variables:
                    #
                    # short flagGraupelHail(fpdim);
                    #     long_name = "Graupel or Hail flag, only available for DPR FS scan";
                    # short flagHail(fpdim);
                    #     long_name = "0 Hail not detected 1 Hail detected, only available for DPR FS scan";
                    # short flagHeavyIcePrecip(elevationAngle, fpdim);
                    #     long_name = "Flag for heavyIcePrecip, only available for DPR FS scan";
                    # float mixedPhaseTop(fpdim);
                    #     long_name = "DPR detected top of mixed phase, only available for DPR FS scan (MSL)";units = "m";

                    if isVersion7:
                        if not data_is_dprgmi:
                            varDict_fpdim['flagInversion'] = nc.variables['flagInversion'][:]  # fpdim
                            varDict_elev_fpdim['precipWater'] =  nc.variables['precipWater'+add][:]
                            if SCAN_TYPE.lower() == 'fs_ku' and GPM_SENSOR.lower() == 'dpr':
                                varDict_fpdim['flagGraupelHail'] = nc.variables['flagGraupelHail'][:]  # fpdim
                                varDict_fpdim['flagHail'] = nc.variables['flagHail'][:]  # fpdim
                                varDict_fpdim['mixedPhaseTop'] = nc.variables['mixedPhaseTop'][:]  # fpdim
                                varDict_elev_fpdim['flagHeavyIcePrecip'] = nc.variables['flagHeavyIcePrecip' + add][:]
                                varDict_fpdim['nHeavyIcePrecip'] = nc.variables['nHeavyIcePrecip'][:]  # fpdim
                                varDict_elev_fpdim['finalDFR'] = nc.variables['finalDFR' + add][:]
                                varDict_elev_fpdim['measuredDFR'] = nc.variables['measuredDFR' + add][:]

                            else:
                                # make these missing so other scans don't need separate databases
                                varDict_fpdim['flagGraupelHail'] = np.empty_like(varDict_fpdim['piaFinal'])
                                varDict_fpdim['flagGraupelHail'][:] = -9999.0
                                varDict_fpdim['flagHail'] = np.empty_like(varDict_fpdim['piaFinal'])
                                varDict_fpdim['flagHail'][:] = -9999.0
                                varDict_fpdim['mixedPhaseTop'] = np.empty_like(varDict_fpdim['piaFinal'])
                                varDict_fpdim['mixedPhaseTop'][:] = -9999.0
                                varDict_elev_fpdim['flagHeavyIcePrecip'] = np.empty_like(varDict_elev_fpdim['Nw'])
                                varDict_elev_fpdim['flagHeavyIcePrecip'][:] = -9999.0
                                varDict_fpdim['nHeavyIcePrecip'] = np.empty_like(varDict_fpdim['piaFinal'])
                                varDict_fpdim['nHeavyIcePrecip'][:] = -9999.0
                                varDict_elev_fpdim['finalDFR'] = np.empty_like(varDict_elev_fpdim['Nw'])
                                varDict_elev_fpdim['finalDFR'][:] = -9999.0
                                varDict_elev_fpdim['measuredDFR'] = np.empty_like(varDict_elev_fpdim['Nw'])
                                varDict_elev_fpdim['measuredDFR'][:] = -9999.0

                    # pick variable with most dimensions to define loops
                    elevations = nc.variables['GR_HID'+add].shape[0]
                    fpdim = nc.variables['GR_HID'+add].shape[1]
                    hidim = nc.variables['GR_HID'+add].shape[2]

                    count=0
                    site_rainy_count = 0
                    for fp in range(fpdim):
                        for elev in range(elevations):
                            #if varDict_elev_fpdim['PrecipRate'][elev][fp] >= precip_rate_thresh:
                            if data_is_dprgmi:
                                if varDict_elev_fpdim['precipTotRate'][elev][fp] >= precip_rate_thresh or \
                                        varDict_elev_fpdim['GR_RC_rainrate'][elev][fp] >= precip_rate_thresh:
                                    site_rainy_count = site_rainy_count + 1
                                    break
                            else:
                                if varDict_elev_fpdim['PrecipRate'][elev][fp] >= precip_rate_thresh or varDict_elev_fpdim['GR_RC_rainrate'][elev][fp] >= precip_rate_thresh:
                                    site_rainy_count = site_rainy_count + 1
                                    break
                    #print("rainy count ", site_rainy_count)
                    #print("fpdim ", fpdim)
                    #percent_rainy = float(site_rainy_count)/float(fpdim)
                    percent_rainy = 100.0 * float(site_rainy_count)/float(fpdim)
                    #print ("percent rainy ", percent_rainy)

                    # mean_zeroDeg = compute_mean_height(zero_altitude)
                    # # compute mean BB
                    # if data_is_dprgmi:
                    #     meanBB = mean_zeroDeg
                    # else:
                    #     meanBB = compute_mean_BB(varDict_fpdim['BBheight'], varDict_fpdim['BBstatus'],nc.variables['TypePrecip'])
                    # if meanBB < 0.0:
                    #     if mean_zeroDeg > 0.0:
                    #         meanBB = mean_zeroDeg
                    #     else:
                    #         if alt_BB_height > 0.0:
                    #             meanBB = alt_BB_height
                    #             print("missing Bright band, using Ruc_0 height ", meanBB)
                    #         else:
                    #             meanBB = -9999.0
                    #             print("missing Bright band and Ruc_0 height...")
                    # else:
                    #     print("Mean Bright band ", meanBB)

                    for elev in range(elevations):
                        for fp in range(fpdim):
                            # only use matchup volumes > minimimum rain rate
                            if data_is_dprgmi:
                                if varDict_elev_fpdim['precipTotRate'][elev][fp] < precip_rate_thresh and \
                                        varDict_elev_fpdim['GR_RC_rainrate'][elev][fp] < precip_rate_thresh:
                                    continue
                            else:
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

                            # fp_entry={"time": closestTime, "elev":float(elevationAngle[elev]),
                            #           "vn_filename":VN_filename, "site_percent_rainy":percent_rainy,
                            #           "site_rainy_count":site_rainy_count, "site_fp_count":fpdim,
                            #           "site_elev":site_elev, "meanBB":meanBB, "fp":fp}
                            fp_entry={"time": closestTime,
                                      "site_percent_rainy":percent_rainy,
                                      "site_rainy_count":site_rainy_count, "site_fp_count":fpdim}
                            #          "fp":fp}
                            #if data_is_dprgmi:
                            #    fp_entry["ruc_0_height"] = float(ma.getdata(zero_altitude)[fp])
                            #else:
                            #    fp_entry["ruc_0_height"] = alt_BB_height


                            # if mean_zeroDeg > 0.0: # we have a mean value so there are some valid individual values
                            #     fp_entry["zero_deg_height"] = float(ma.getdata(zero_altitude)[fp])
                            # else:
                            #     fp_entry["zero_deg_height"] = alt_BB_height
                            #
                            if data_is_dprgmi:
                                mx = np.ma.masked_invalid(ma.getdata(nc.variables['zeroDegAltitude']))
                                if not mx[fp]:
                                    fp_entry['zeroDegAltitude'] = -9999.0
                                else:
                                    fp_entry['zeroDegAltitude'] = float(ma.getdata(nc.variables['zeroDegAltitude' + add])[fp])
                            else:  # added zero_altitude for DPR, Ku, Ka
                                #if np.isnan(ma.getdata(nc.variables['heightZeroDeg'])[fp]):
                                mx = np.ma.masked_invalid(ma.getdata(nc.variables['heightZeroDeg']))
                                if not mx[fp]:
                                    print("heightZeroDeg is NaN ")
                                    fp_entry['heightZeroDeg'] = -9999.0
                                else:
                                    fp_entry['heightZeroDeg'] = float(ma.getdata(nc.variables['heightZeroDeg'])[fp])
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
                            hid_vals=[]
                            for hid_ind in range(hidim-4): # leave off three spare bins and dropped 'HR' category
                            #for hid_ind in range(hidim - 3):  # leave off three spare bins
                                hid_val = int(ma.getdata(hid[elev][fp])[hid_ind])
                                #fp_entry["hid_"+str(hid_ind+1)] = int(ma.getdata(hid[elev][fp])[hid_ind])
                                fp_entry["hid_" + str(hid_ind + 1)] = hid_val
                                hid_vals.append(hid_val)
                            bottom_ht = (float(ma.getdata(nc.variables['bottomHeight'+add][elev])[fp]) + site_elev) * 1000.0
                            top_ht = (float(ma.getdata(nc.variables['topHeight'+add][elev])[fp]) + site_elev) * 1000.0
                            fp_entry['liquid_hid_flag'] = compute_liquid_hid_flag(bottom_ht, freezing_level_height, hid_vals)
                            if have_blockage == 1:
                                fp_entry['GR_blockage'] = float(ma.getdata(nc.variables['GR_blockage'+add][elev])[fp])
                            else:
                                fp_entry['GR_blockage'] = -9999.0

                            # compute BB proximity
                            #            varDict_elev_fpdim['topHeight'] = nc.variables['topHeight'][:] # elevAngle 	fpdim
                            #               varDict_elev_fpdim['bottomHeight'] = nc.variables['bottomHeight'][:] # elevAngle 	fpdim

                            # bbprox = compute_BB_prox(meanBB, float(ma.getdata(varDict_elev_fpdim['topHeight'][elev])[fp]),
                            #                          float(ma.getdata(varDict_elev_fpdim['bottomHeight'][elev])[fp]))
                            bbprox = compute_BB_prox(freezing_level_height, top_ht, bottom_ht)
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

                            fp_entry['n_samples_z'] = gr_exp - gr_rej
                            #print ("dpr_exp ", dpr_exp, " dpr_rej ",dpr_rej)
                            if gr_exp > 0:
                                fp_entry['GR_beam'] = 100.0 * (gr_exp - gr_rej)/gr_exp
                            else:
                                fp_entry['GR_beam'] = 0.0
                            if dpr_exp > 0:
                                fp_entry['DPR_beam'] = 100.0 * (dpr_exp - dpr_rej)/dpr_exp
                            else:
                                fp_entry['DPR_beam'] = 0.0
                            fp_entry['freezing_level_height'] = freezing_level_height
                            # put last in record for partitioning
                            fp_entry['GPM_ver']=GPM_VERSION
                            #fp_entry['VN_ver']=vn_version
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
        raise err
#        return {'error': 'Error occurred during process_file'}

    gz.close()

    return have_mrms, outputJson
