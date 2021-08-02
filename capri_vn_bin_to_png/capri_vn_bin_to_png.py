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

import boto3 as boto3
from urllib.parse import unquote_plus

from lxml import etree, objectify
from pykml.factory import KML_ElementMaker as KML

import numpy as np

from matplotlib import pyplot as plt, ticker
from matplotlib.colors import LinearSegmentedColormap, ListedColormap, LogNorm

import botocore
from botocore.exceptions import ClientError

from colormap import colormap
from mrmslib import binaryFile

s3 = boto3.resource(
    's3')


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


def lambda_handler(event, context):

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        file = unquote_plus(record['s3']['object']['key'])
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

        # eventually, plan on these being defined as environment variables
        config = {
            "IMG_DIR": "img",
            "BIN_DIR": "bin",
            "s3_bucket_out": "capri-vn-data",
        }
        img_dir = config["IMG_DIR"]
        bin_dir = config["BIN_DIR"]
        bucket_out = config["s3_bucket_out"]

        # download s3 file to /tmp storage
        try:
            s3.Bucket(bucket).download_file(file, '/tmp/'+ fn)
        except botocore.exceptions.ClientError as e:
            print("Error reading the s3 object " + file)
            exit(-1)

        print('processing file: ' + file)

        binfile = binaryFile('/tmp/'+ fn)
        # crop from edges to make 256 x 256 size image for DL experiments
        binfile.crop_from_center(256,256)
        binfile.write('/tmp/subset.bin')

        if (skip_fp):
            # upload .bin image to S3 bucket
            print('processing of png skipped for footprint file ' + file)
            s3.Bucket(bucket_out).upload_file('/tmp/subset.bin', bin_dir+ '/'+ fn)
            return

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
        plt.imsave('/tmp/'+'log.png', log_data, cmap=cm, vmin=-1, vmax=np.log10(100.0))
        plt.imsave('/tmp/'+'linear.png', data, cmap=cm, vmin=0.0, vmax=60.0)
        plt.imsave('/tmp/'+'log_bw.png', log_data, cmap=bm, vmin=-1, vmax=np.log10(100.0))
        plt.imsave('/tmp/'+'linear_bw.png', data, cmap=bm, vmin=0.0, vmax=60.0)

        col_kml = save_kml('/tmp', fn_base + ".col.png", north, south, east, west, data_desc)
        col_log_kml = save_kml('/tmp', fn_base + ".col_log.png", north, south, east, west, data_desc)
        bw_kml = save_kml('/tmp', fn_base + ".bw.png", north, south, east, west, data_desc)
        bw_log_kml = save_kml('/tmp', fn_base + ".bw_log.png", north, south, east, west, data_desc)

        # upload final files to image directory on S3
        print("uploading files to S3:")
        try:
            # upload .png files
            s3.Bucket(bucket_out).upload_file("/tmp/" +'linear.png', img_dir+ '/'+ fn_base + ".col.png")
            s3.Bucket(bucket_out).upload_file("/tmp/" +'log.png', img_dir+ '/'+ fn_base + ".col_log.png")
            s3.Bucket(bucket_out).upload_file("/tmp/" +'linear_bw.png', img_dir+ '/'+ fn_base + ".bw.png")
            s3.Bucket(bucket_out).upload_file("/tmp/" +'log_bw.png', img_dir+ '/'+ fn_base + ".bw_log.png")

            # upload .kml files
            s3.Bucket(bucket_out).upload_file(col_kml, img_dir+ '/'+ fn_base + ".col.kml")
            s3.Bucket(bucket_out).upload_file(col_log_kml, img_dir+ '/'+ fn_base + ".col_log.kml")
            s3.Bucket(bucket_out).upload_file(bw_kml, img_dir+ '/'+ fn_base + ".bw.kml")
            s3.Bucket(bucket_out).upload_file(bw_log_kml, img_dir+ '/'+ fn_base + ".bw_log.kml")

            # upload .bin image to S3 bucket
            s3.Bucket(bucket_out).upload_file('/tmp/subset.bin', bin_dir+ '/'+ fn)

        except botocore.exceptions.ClientError as e:
            print("Error uploading the s3 object " + e.response)
            exit(-1)


        # list of all files for a given radar on a give day:
        # GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz
        # GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz.fp.bin
        # GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz.gpm.bin
        # GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz.gpm.bw.kml
        # GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz.gpm.bw.png
        # GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz.gpm.col.kml
        # GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz.gpm.col.png
        # GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz.mrms.bin
        # GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz.mrms.bw.kml
        # GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz.mrms.bw.png
        # GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz.mrms.col.kml
        # GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz.mrms.col.png
        # GRtoDPR.KABR.210114.39090.V06A.DPR.NS.2_0.nc.gz
        # GRtoDPR.KABR.210114.39090.V06A.DPR.NS.2_0.nc.gz.fp.bin
        # GRtoDPR.KABR.210114.39090.V06A.DPR.NS.2_0.nc.gz.gpm.bin
        # GRtoDPR.KABR.210114.39090.V06A.DPR.NS.2_0.nc.gz.gpm.bw.kml
        # GRtoDPR.KABR.210114.39090.V06A.DPR.NS.2_0.nc.gz.gpm.bw.png
        # GRtoDPR.KABR.210114.39090.V06A.DPR.NS.2_0.nc.gz.gpm.col.kml
        # GRtoDPR.KABR.210114.39090.V06A.DPR.NS.2_0.nc.gz.gpm.col.png
        # GRtoDPR.KABR.210114.39090.V06A.DPR.NS.2_0.nc.gz.mrms.bin
        # GRtoDPR.KABR.210114.39090.V06A.DPR.NS.2_0.nc.gz.mrms.bw.kml
        # GRtoDPR.KABR.210114.39090.V06A.DPR.NS.2_0.nc.gz.mrms.bw.png
        # GRtoDPR.KABR.210114.39090.V06A.DPR.NS.2_0.nc.gz.mrms.col.kml
        # GRtoDPR.KABR.210114.39090.V06A.DPR.NS.2_0.nc.gz.mrms.col.png

        # e.g. GRtoDPR.KABR.210106.38967.V06A.DPR.NS.2_0.nc.gz.gpm.bin
