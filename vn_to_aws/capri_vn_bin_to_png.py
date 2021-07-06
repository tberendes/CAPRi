# ---------------------------------------------------------------------------------------------
#
#  vn_bin_to_png.py
#
#  Author:  Todd Berendes, UAH ITSC, June 2021
#
#  Description: this program reads and converts a binary VN rain rate file to color indexed PNG
#  using both linear and log scales in color and B/W
#  Currently, the color bars are set up for 0 to 60 (linear) and -1 to 100 (log) ranges (rain rate).
#
#  Syntax: currently no input parameters
#
# ---------------------------------------------------------------------------------------------

import vnlib
import boto3 as boto3
from urllib.parse import unquote_plus

import numpy as np

from matplotlib import pyplot as plt, ticker
from matplotlib.colors import LinearSegmentedColormap, ListedColormap, LogNorm

import botocore
from botocore.exceptions import ClientError

from vnlib import colormap

s3 = boto3.resource(
    's3')
def lambda_handler(event, context):

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        file = unquote_plus(record['s3']['object']['key'])
        fn = file.split('/')[-1]
        print('file name '+file)
        print('basename '+fn)

        # eventually, plan on these being defined as environment variables
        config = {
            "IMG_DIR": "img",
            "s3_bucket_out": "capri-vn-data",
        }
        img_dir = config["IMG_DIR"]
        bucket_out = config["s3_bucket_out"]

        # download s3 file to /tmp storage
        try:
            s3.Bucket(bucket).download_file(file, '/tmp/'+ fn)
        except botocore.exceptions.ClientError as e:
            print("Error reading the s3 object " + file)
            exit(-1)

        print('processing file: ' + file)

        binfile = vnlib.binaryFile('/tmp/'+ fn)
        binfile.set_flip_flag(True)  # set to access data in image coordinates from ascending Lat
        data = binfile.get_data()

        colors = []
        bw = []
        ind = 0
        for row in colormap.colormap.data:
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

        # clamp values at 60
        b = np.where(data > 60)
        data[b] = 60

        b = np.where(data > 0)
        min = np.min(data[b])
        b = np.where(data < min)
        # mrms_data[b]=min_mrms
        data[b] = -9999
        print("max ", np.max(data))
        print("min ", np.min(data))

        log_data = np.copy(data)

        a = np.where(log_data > 0)
        log_data[a] = np.log10(log_data[a])

        plt.imsave('/tmp/'+'log.png', log_data, cmap=cm, vmin=-1, vmax=np.log10(100.0))
        plt.imsave('/tmp/'+'linear.png', data, cmap=cm, vmin=0.0, vmax=60.0)
        plt.imsave('/tmp/'+'log_bw.png', log_data, cmap=bm, vmin=-1, vmax=np.log10(100.0))
        plt.imsave('/tmp/'+'linear_bw.png', data, cmap=bm, vmin=0.0, vmax=60.0)

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

        # make output filenames

        # upload final files to image directory on S3
        fn_base = fn.split('.bin')[0]
        print("uploading file to S3:")
        try:
            s3.Bucket(bucket_out).upload_file("/tmp/" +'linear.png', img_dir+ '/'+ fn_base + "col.png")
            s3.Bucket(bucket_out).upload_file("/tmp/" +'log.png', img_dir+ '/'+ fn_base + "col_log.png")
            s3.Bucket(bucket_out).upload_file("/tmp/" +'linear_bw.png', img_dir+ '/'+ fn_base + "bw.png")
            s3.Bucket(bucket_out).upload_file("/tmp/" +'log_bw.png', img_dir+ '/'+ fn_base + "bw_log.png")
        except botocore.exceptions.ClientError as e:
            print("Error uploading the s3 object " + e.response)
            exit(-1)


