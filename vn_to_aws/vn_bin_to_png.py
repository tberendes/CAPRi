# ---------------------------------------------------------------------------------------------
#
#  capri_vn_bin_to_png.py
#
#  Author:  Todd Berendes, UAH ITSC, July 2021
#
#  Description: this program reads and converts a binary VN rain rate file to color indexed PNG
#  using both linear and log scales in color and B/W
#  Currently, the color bars are set up for 0 to 60 (linear) and -1 to 100 (log) ranges (rain rate).
#
#  Syntax: currently no input parameters
#
# ---------------------------------------------------------------------------------------------
import math

from urllib.parse import unquote_plus
import sys

from lxml import etree, objectify
from pykml.factory import KML_ElementMaker as KML

import numpy as np

from matplotlib import pyplot as plt, ticker
from matplotlib.colors import LinearSegmentedColormap, ListedColormap, LogNorm

from colormap import colormap
from mrmslib import binaryFile

import os

def save_kml(dirname, png_file, north, south, east, west, data_desc):

    llbox = KML.LatLonBox(KML.north(north), KML.south(south), KML.east(east), KML.west(west))
    icon = KML.Icon(href=png_file)
    grnd = KML.GroundOverlay(KML.name(png_file), KML.description(data_desc), icon, llbox)
    fld = KML.Folder(KML.name("GPM Validation Network"), KML.description("Rain Rate"), grnd)

    #print(etree.tostring(fld, pretty_print=True).decode('UTF-8'))
    objectify.deannotate(fld, cleanup_namespaces=True)

    out_fn = dirname+'/'+png_file+'.kml'
    with open(out_fn, "w") as f:
        print('<?xml version="1.0" encoding="UTF-8"?>' + '\n' + etree.tostring(fld, pretty_print=True).decode('UTF-8'),
              file=f)

    return out_fn


def main():

    if len(sys.argv) < 5:
 #       print("Usage: python vn_to_parquet_partition.py <vn_base_dir> <output_base_dir> <BB_filename> <start YYMMDD> <end YYMMDD>")
        print("Usage: python vn_bin_to_png.py <vn_base_dir> <output_base_dir> <start YYMMDD> <end YYMMDD>")
        print("Note: start and end dates are inclusive")
        sys.exit()

    print('start...')
    VN_DIR = sys.argv[1]
    OUT_DIR = sys.argv[2]

    # testing:
#    VN_DIR = '/data/v7_test/mrms/netcdf/geo_match/GPM/2ADPR/FS_Ku/V07A/2_3/2014/'
#    OUT_DIR = '/data/v7_test/mrms/'

    START_DATE = int(sys.argv[3])
    END_DATE = int(sys.argv[4])

    for root, dirs, files in os.walk(VN_DIR, topdown=False):
        for file in files:

            #print('file ' + file)
            #only process .bin files
            if not file.endswith('.bin'):
                #print('skipping file: ' + file)
                continue

            if file.endswith('fp.bin'): # skip footprint files
                #print('skipping file: ' + file)
                continue

            site = file.split('.')[1]
            file_date = int(file.split('.')[2])
            if not ( file_date >= START_DATE and file_date <= END_DATE):
                #print('skipping file: ' + file)
                continue

            fn = file.split('/')[-1]
            #print('file name '+file)
            #print('basename '+fn)

            # e.g. GRtoDPR.KABR.140321.334.V06A.DPR.NS.1_21.nc.gz.gpm.bin
            data_type = fn.split('.')[-2]
            #print('data type '+data_type)

            skip_fp = False
            if (data_type.upper()=='GPM'):
                data_desc = 'GPM Near Surface Rain Rate'
            elif (data_type.upper()=='MRMS'):
                data_desc = 'MRMS Surface Rain Rate'
            else:
                skip_fp = True

        #  https://s3.console.aws.amazon.com/s3/object/capri-model-data?region=us-east-1&prefix=checkpoint-2500/GRtoDPR.KABR.140321.334.V06A.DPR.NS.2_0.nc.gz.model.bin
            # eventually, plan on these being defined as environment variables
            # set defaults in case environment variables aren't set
            config = {
                "IMG_DIR": "img",
                "BIN_DIR": "bin",
            }
            env_vars = os.environ
            has_items = bool(env_vars)
            if (has_items):
                if "IMG_DIR" in env_vars:
                    print('using IMG_DIR='+env_vars["IMG_DIR"])
                    config["IMG_DIR"] = env_vars["IMG_DIR"]
                # else:
                #     print('default IMG_DIR=' + config["IMG_DIR"])
                if "BIN_DIR" in env_vars:
                    print('using BIN_DIR='+env_vars["BIN_DIR"])
                    config["BIN_DIR"] = env_vars["BIN_DIR"]
                # else:
                #     print('default BIN_DIR=' + config["BIN_DIR"])

            img_dir = config["IMG_DIR"]
            bin_dir = config["BIN_DIR"]

            print('processing file: ' + file)

            binfile = binaryFile(os.path.join(root, file))
            # crop from edges to make 256 x 256 size image for DL experiments
            binfile.crop_from_center(256,256)

            # make paths if necessary
            os.makedirs(OUT_DIR + '/' + config["BIN_DIR"] + '/'+ site ,exist_ok=True)
            binfile.write(OUT_DIR+'/'+config["BIN_DIR"]+'/'+ site +'/' + file)

            binfile.set_flip_flag(True)  # set to access data in image coordinates from ascending Lat
            data = binfile.get_data()
            north, south, east, west = binfile.get_lat_lon_box() # get lat/lon bounds

            colors = []
            bw = []
            ind = 0
            for row in colormap.data:
                if ind == 0:
                    entry = (float(row[0]) / 255.0, float(row[1]) / 255.0, float(row[2]) / 255.0, 0.0)
                    #            entry = (0.0,0.0,0.0,0.0)
                    colors.append(entry)
                    entry = (float(ind) / 255.0, float(ind) / 255.0, float(ind) / 255.0, 0.0)
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
            bm = ListedColormap(bw, N=256)
            #    plt.imshow(np.random.random((100, 100))*60, cmap=plt.cm.BuPu_r)
            # plt.imshow(np.random.random((100, 100)) * 60, cmap=cm)

            log_data = np.copy(data)
            for row in range(log_data.shape[0]):
                for col in range(log_data.shape[1]):
                    if log_data[row][col] > 0:
                        log_data[row][col] = np.log10(log_data[row][col])
                    else:
                        log_data[row][col] = -9999.0

            fn_base = fn.split('.bin')[0]

            # make paths if necessary

            os.makedirs(OUT_DIR + '/' + config["IMG_DIR"]+'/' + site +'/' + 'color_log',exist_ok=True)
            os.makedirs(OUT_DIR + '/' + config["IMG_DIR"]+'/' + site +'/' + 'color_linear',exist_ok=True)
            os.makedirs(OUT_DIR + '/' + config["IMG_DIR"]+'/' + site +'/' + 'bw_log',exist_ok=True)
            os.makedirs(OUT_DIR + '/' + config["IMG_DIR"]+'/' + site +'/' + 'bw_linear',exist_ok=True)

            plt.imsave(OUT_DIR+'/' + config["IMG_DIR"]+'/' + site +'/' + 'color_log/' + fn_base + ".png", log_data, cmap=cm, vmin=-1, vmax=np.log10(100.0))
            plt.imsave(OUT_DIR+'/' + config["IMG_DIR"]+'/' + site +'/' + 'color_linear/' + fn_base + ".png", data, cmap=cm, vmin=0.0, vmax=60.0)
            plt.imsave(OUT_DIR+'/' + config["IMG_DIR"]+'/' + site +'/' + 'bw_log/' + fn_base + ".png", log_data, cmap=bm, vmin=-1, vmax=np.log10(100.0))
            plt.imsave(OUT_DIR+'/' + config["IMG_DIR"]+'/' + site +'/' + 'bw_linear/' + fn_base + ".png", data, cmap=bm, vmin=0.0, vmax=60.0)

            col_log_kml = save_kml(OUT_DIR+'/' + config["IMG_DIR"]+'/' + site +'/' + 'color_log/', fn_base + ".png", north, south, east, west, data_desc)
            col_kml = save_kml(OUT_DIR+'/' + config["IMG_DIR"]+'/' + site +'/' + 'color_linear/' , fn_base + ".png", north, south, east, west, data_desc)
            bw_log_kml = save_kml(OUT_DIR+'/' + config["IMG_DIR"]+'/' + site +'/' + 'bw_log/' , fn_base + ".png", north, south, east, west, data_desc)
            bw_kml = save_kml(OUT_DIR+'/' + config["IMG_DIR"]+'/' + site +'/' + 'bw_linear/', fn_base + ".png", north, south, east, west, data_desc)

if __name__ == '__main__':
   main()
