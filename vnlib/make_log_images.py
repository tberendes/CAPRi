# ---------------------------------------------------------------------------------------------
#
#  make_log_images.py
#
#  Author:  Todd Berendes, UAH ITSC, April 2021
#
#  Description: this script reads MRMS and GPM binary files images generated by MRMS to VN matchups
#               and creates output png images of MRMS and GPM data using log scaling
#
#  Syntax: currently no input parameters
#
#
#  To Do: modify to accept input parameters
#
# ---------------------------------------------------------------------------------------------

# --Do all the necessary imports
import os
import datetime

import numpy as np
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, ListedColormap

import vnlib
import colormap

def plot_with_log(title, data, log_data, cm):
    plt.clf()
    plt.cla()
    plt.subplot(211, xticks=[], yticks=[])
    #    plt.subplot(221)
    plt.imshow(data, cmap=cm, vmin=0.0, vmax=60.0)
    # plt.imshow(log_mrms, cmap=cm, vmin=0.01, vmax=np.log10(60.0)))
    cax1 = plt.axes([0.65, 0.53, 0.075, 0.36])
    cb1 = plt.colorbar(cax=cax1)
    cb1.set_label('Rain rate [mm/hr]', rotation=-90, color='k', labelpad=20)
    # cb.minorticks_on()
    cb1.set_ticks([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60], update_ticks=True)
    # cb.set_ticks([0,10,20,30,40,50,60], update_ticks=True)
    cb1.update_ticks()
    plt.subplot(212, xticks=[], yticks=[])
    #    plt.subplot(223)
    plt.imshow(log_data, cmap=cm, vmin=-1, vmax=np.log10(100.0))
    # plt.imshow(gpm_data, cmap=cm, vmin=0.0, vmax=60.0)
    # plt.imshow(log_gpm, cmap=cm, vmin=0.01, vmax=np.log10(60.0))
    # plt.imshow(np.random.random((100, 100)) * 60, cmap=cm)

    cax2 = plt.axes([0.65, 0.1, 0.075, 0.36])
    cb2 = plt.colorbar(cax=cax2)
    cb2.set_label('Log10(Rain rate) log10[mm/hr]', rotation=-90, color='k', labelpad=20)
    # cb.minorticks_on()
    cb2.set_ticks([-1, 0, 1, 2], update_ticks=True)
    # cb.set_ticks([0,10,20,30,40,50,60], update_ticks=True)
    cb2.update_ticks()

    plt.subplots_adjust(bottom=0.1, right=0.7, top=0.9)
    plt.figtext(0.5, 0.95, title, horizontalalignment='center')
    # cax = plt.axes([0.75, 0.1, 0.075, 0.8])

    # cax1 = plt.axes([0.75, 0.1, 0.075, 0.4])
    # cb = plt.colorbar(cax=cax1)
    # cb.set_label('Rain rate [mm/hr]', rotation=-90, color='k', labelpad=20)
    # #cb.minorticks_on()
    # cb.set_ticks([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60], update_ticks=True)
    # # cb.set_ticks([0,10,20,30,40,50,60], update_ticks=True)
    # cb.update_ticks()


def main():

    # uncomment this to pass parameters
    # if len(sys.argv)>1:
    #     mrms_filename = sys.argv[1]
    # else:
    #     print("usage: query_vn_by_fp /path/to/mrms_filename.bin")
    #     sys.exit(-1)

    #testing
    mrms_filename = '/data/capri_test_data/VN/mrms_geomatch/2020/GRtoDPR.KABR.200312.34295.V06A.DPR.NS.1_21.nc.gz.mrms.bin'

    parsed_by_dot=mrms_filename.split('.')
    site=parsed_by_dot[1]
    date=parsed_by_dot[2]
    # load mrms, gpm, and footprint .bin files int MRMSToGPM class variable MRMSMatch
    # assumes filename format used in the Java VN matchup program for MRMS data
    # and also assumes that the footprint and GPM images (.bin files) are in the same directory
    MRMSMatch = vnlib.MRMSToGPM(mrms_filename)

    MRMSMatch.set_flip_flag(True) # set to access data in image coordinates
    fp_data = MRMSMatch.GPMFootprint.get_data()
    mrms_data = MRMSMatch.MRMS.get_data()
    gpm_data = MRMSMatch.GPM.get_data()

    print('fp ', fp_data[0][0], ' gpm ', gpm_data[0][0], ' mrms ', mrms_data[0][0])
    (lat,lon) = MRMSMatch.MRMS.get_lat_lon(0,0)
    print('lat ', lat, ' lon ', lon)

    # use fields from the filename to query database to get only matching site data
    #GRtoDPR.KABR.200312.34295.V06A.DPR.NS.1_21.nc.gz.mrms.bin
    # Parse filename to set query parameters to retrieve VN data from Athena
    parsed = mrms_filename.split('.')
    site = parsed[1]
    date_field = parsed[2]
    year = '20' + date_field[0:2]
    month = date_field[2:4]
    day = date_field[4:6]
    # may want to add orbit number to database
    orbit = parsed[3]
    # for now do a "like" match on the begining of the filename up to the orbit number
    gpm_version = parsed[4]
    vn_fn = os.path.basename(mrms_filename).split('.' + gpm_version + '.')[0] + '.'
    sensor = parsed[5]
    scan = parsed[6]
    vn_version = parsed[7].replace('_', '.')

    colors = []
    bw = []
    ind = 0
    for row in colormap.colormap.data:
        if ind == 0:
            entry = (0.0,0.0,0.0,0.0)
            colors.append(entry)
            bw.append(entry)
        else:
            entry = (float(row[0]) / 255.0, float(row[1]) / 255.0, float(row[2]) / 255.0, 1.0)
            colors.append(entry)
            entry = (float(ind) / 255.0, float(ind) / 255.0, float(ind) / 255.0, 1.0)
            bw.append(entry)
        ind = ind + 1

    # color
#    cm = LinearSegmentedColormap.from_list('Colors', colors, N=256)
    cm = ListedColormap(colors, N=256)
    #    plt.imshow(np.random.random((100, 100))*60, cmap=plt.cm.BuPu_r)
    #plt.imshow(np.random.random((100, 100)) * 60, cmap=cm)


    # clamp values at 60
    b = np.where(mrms_data > 60)
    mrms_data[b]=60

    b = np.where(mrms_data > 0)
    min_mrms = np.min(mrms_data[b])
    b = np.where(mrms_data < min_mrms)
    #mrms_data[b]=min_mrms
    mrms_data[b]=-9999
    print ("max mrms", np.max(mrms_data))
    print ("min mrms", np.min(mrms_data))

    b = np.where(gpm_data > 60)
    gpm_data[b]=60

    # need to set up RGB values directly, not using colormaps so I can set missing to black
#    missing = np.where(gpm_data < 0)
    b = np.where(gpm_data > 0)
    min_gpm = np.min(gpm_data[b])
    b = np.where(gpm_data < min_gpm)
    #gpm_data[b]=min_gpm
    gpm_data[b]=-9999
    print ("max gpm", np.max(gpm_data))
    print ("min gpm", np.min(gpm_data))


    log_gpm = np.copy(gpm_data)
    log_mrms = np.copy(mrms_data)

    a = np.where(log_gpm > 0)
    log_gpm[a] = np.log10(log_gpm[a])
    a = np.where(log_mrms > 0)
    log_mrms[a] = np.log10(log_mrms[a])

#    plt.imsave('mrms_log.png', log_mrms, cmap=cm, vmin=0.01, vmax=np.log10(60.0))
    plt.imsave('mrms_log.png', log_mrms, cmap=cm, vmin=-1, vmax=np.log10(100.0))
    plt.imsave('mrms.png', mrms_data, cmap=cm, vmin=0.0, vmax=60.0)
    plt.imsave('gpm_log.png', log_gpm, cmap=cm, vmin=-1, vmax=np.log10(100.0))
    plt.imsave('gpm.png', gpm_data, cmap=cm, vmin=0.0, vmax=60.0)

    plot_with_log('MRMS ' + site +' ' + date, mrms_data, log_mrms, cm)
    plt.savefig('mrms_fig.png',dpi = (200))
    plot_with_log('GPM ' + site +' ' + date, gpm_data, log_gpm, cm)
    plt.savefig('gpm_fig.png',dpi = (200))

    # # Now we can loop through MRMSMatch class MRMS gridded GPMFootprint image
    # to retrieve VN query information for each matching MRMS value

    for row in range(MRMSMatch.GPMFootprint.height): # image rows
        for col in range(MRMSMatch.GPMFootprint.width): #image columns
            # do something with each pixel
            fp = int(fp_data[row][col]) # need to convert to int to index in VN_data
            mrms_precip = mrms_data[row][col]
            gpm_precip = gpm_data[row][col]

            # if fp > 0 and fp in VN_data.keys(): # if fp >0 the row,col value for MRMS falls within a GPM footprint in the VN with precip
            #     print("fp: ", fp, " mrms precip ", mrms_precip, " gpm precip ", gpm_precip)
            #     # example loop to extract individual vn volumes from the footprint indexed VN_data class
            #     volumes = VN_data[fp]
            #     for volume in volumes: # loop through individual VN volume in GPM footprint (multiple elevations)
            #         print("   bottomHeight", volume["bottomHeight"], " GR_RC_rainrate ", volume["GR_RC_rainrate"])
            #     # exit early for testing ***********************************
            #     exit(0)
            # else: # MRMS image value is not within a GPM footprint
            #     print("no matching VN volume for MRMS data ")

if __name__ == '__main__':
   main()
