# ---------------------------------------------------------------------------------------------
#
#  capri_vn_wrap_kml.py
#
#  Author:  Todd Berendes, UAH ITSC, July 2021
#
#  Description: this program reads a kml file created by the MRMS to VN matchup process and creates
#  wrapper files for all of the images created by capri_vn_bin_to_png.py
#  Note that only the *.gpm.col.kml file upload triggers the lambda, and the other combinations are
#  constructed from that file, i.e. .gpm.bw.kml .gpm.col_log.kml .gpm.bw_log.kml
#  Also, the four variations for the mrms imagery are created.
#
#  Syntax: currently no input parameters
#
# ---------------------------------------------------------------------------------------------

import vnlib
import boto3 as boto3
from urllib.parse import unquote_plus
from pykml import parser
from lxml import etree, objectify
from pykml.factory import KML_ElementMaker as KML

import numpy as np

from matplotlib import pyplot as plt, ticker
from matplotlib.colors import LinearSegmentedColormap, ListedColormap, LogNorm

import botocore
from botocore.exceptions import ClientError

from vnlib import colormap

s3 = boto3.resource(
    's3')

# note: doc object is not copied, so modifications will affect original object passed into function
def write_kml(png_file, doc, outdir):
    outfn = outdir+'/'+str(png_file).split('.png')[0]+'.kml'
    root = doc.getroot()
    root.Folder.GroundOverlay.name=KML.name(png_file)
    root.Folder.GroundOverlay.Icon.href=KML.href(png_file)

    objectify.deannotate(root, cleanup_namespaces=True)

    with open(outfn, "w") as f:
        print('<?xml version="1.0" encoding="UTF-8"?>' + '\n' + etree.tostring(doc, pretty_print=True,).decode('UTF-8'),
              file=f)
    return outfn

def main():
    test_fn = '/data/capri_test_data/VN/mrms_geomatch/2014/GRtoDPR.KABR.140321.334.V06A.DPR.NS.1_21.nc.gz.gpm.col.kml'
    # read kml file
    with open(test_fn) as f:
        doc = parser.parse(f)

    root = doc.getroot()
    png_file = str(root.Folder.GroundOverlay.name)

    base_name = str(png_file).split('.gpm.col.png')[0]
#    outdir = '/tmp'
    outdir = './'

    gpm_col = png_file
    upload_files = []
    upload_files.append(write_kml(png_file, doc, outdir))
    upload_files.append(write_kml(base_name + '.gpm.col_log.png', doc, outdir))
    upload_files.append(write_kml(base_name + '.gpm.bw.png', doc, outdir))
    upload_files.append(write_kml(base_name + '.gpm.bw_log.png', doc, outdir))

    root.Folder.GroundOverlay.description = KML.description('MRMS Surface Rain Rate')
    upload_files.append(write_kml(base_name + '.mrms.col.png', doc, outdir))
    upload_files.append(write_kml(base_name + '.mrms.col_log.png', doc, outdir))
    upload_files.append(write_kml(base_name + '.mrms.bw.png', doc, outdir))
    upload_files.append(write_kml(base_name + '.mrms.bw_log.png', doc, outdir))

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


# <?xml version="1.0" encoding="UTF-8"?>
# <kml xmlns="http://www.opengis.net/kml/2.2">
#   <Folder>
#     <name>GPM Validation Network</name>
#     <description></description>
#     <GroundOverlay>
#      <name>GRtoDPR.KABR.200312.34295.V06A.DPR.NS.1_21.nc.gz.gpm.bw.png</name>
#       <description>GPM Near Surface Rain Rate</description>
#       <Icon>
#        <href>GRtoDPR.KABR.200312.34295.V06A.DPR.NS.1_21.nc.gz.gpm.bw.png</href>
#       </Icon>
#       <LatLonBox>
#         <north>47.029609</north>
#         <south>43.881988</south>
#         <east>-96.170059</east>
#         <west>-100.65614199999999</west>
#       </LatLonBox>
#     </GroundOverlay>
#   </Folder>
# </kml>

if __name__ == '__main__':
   main()
