# ---------------------------------------------------------------------------------------------
#
# capri_vn_to_db.py
#
#  Description: This lambda function processes a single VN file on S3 formatted in netCDF
#               and parses out variables, writing output to parquet format for Athena.
#               Metadata files (.json) are also created and written to a separate S3 bucket.
#               An external "Bright Band" file is read as either a csv or pickled dictionary (.pcl).
#
#  Syntax: currently no input parameters
#
#  To Do: modify to accept input parameters, such as filename and maybe the location of district coordinates
#
# ---------------------------------------------------------------------------------------------


# --Do all the necessary imports

from urllib.parse import unquote_plus

import boto3 as boto3
import botocore
import json2parquet
from botocore.exceptions import ClientError

import json

import os

s3 = boto3.resource(
    's3')
from extract_vn import read_alt_bb_file, process_file

def s3_file_exists(s3_bucket, s3_key):
    try:
        # head_object will return exception if file does not already exist
#        s3.head_object(Bucket=s3_bucket, Key=s3_key)
        s3.Object(s3_bucket, s3_key).load()
        print("file " + s3_key + " is already in S3 bucket " + s3_bucket )
        exists = True
    except:
        print("file " + s3_key + " doesn't exist in S3 bucket " + s3_bucket + " ...")
        exists = False
    return exists

def lambda_handler(event, context):

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        file = unquote_plus(record['s3']['object']['key'])
        fn = file.split('/')[-1]
        print('file name '+file)
        print('basename '+fn)

        # eventually, plan on these being defined as environment variables
        # set default values if environment variable are missing
        config = {
            "OUT_DIR": "parquet",
            "META_DIR": "metadata",
            "alt_bb_bucket": "capri-data",
            "alt_bb_file": "vn_mirror/BB/GPM_rain_event_bb_km.txt.pcl",
            "s3_bucket_out": "capri-vn-data",
            "overwrite_s3": False,
        }
        env_vars = os.environ
        has_items = bool(env_vars)
        if (has_items):
            if "OUT_DIR" in env_vars:
                print('using OUT_DIR='+env_vars["OUT_DIR"])
                config["OUT_DIR"] = env_vars["OUT_DIR"]
            else:
                print('default OUT_DIR=' + config["OUT_DIR"])
            if "META_DIR" in env_vars:
                print('using META_DIR='+env_vars["META_DIR"])
                config["META_DIR"] = env_vars["META_DIR"]
            else:
                print('default META_DIR=' + config["META_DIR"])
            if "alt_bb_bucket" in env_vars:
                print('using alt_bb_bucket='+env_vars["alt_bb_bucket"])
                config["alt_bb_bucket"] = env_vars["alt_bb_bucket"]
            else:
                print('default alt_bb_bucket=' + config["alt_bb_bucket"])
            if "alt_bb_file" in env_vars:
                print('using alt_bb_file='+env_vars["alt_bb_file"])
                config["alt_bb_file"] = env_vars["alt_bb_file"]
            else:
                print('default alt_bb_file=' + config["alt_bb_file"])
            if "s3_bucket_out" in env_vars:
                print('using s3_bucket_out='+env_vars["s3_bucket_out"])
                config["s3_bucket_out"] = env_vars["s3_bucket_out"]
            else:
                print('default s3_bucket_out=' + config["s3_bucket_out"])
            if "overwrite_s3" in env_vars:
                print('using overwrite_s3='+env_vars["overwrite_s3"])
                config["overwrite_s3"] = env_vars["overwrite_s3"]
            else:
                print('default overwrite_s3=' + config["overwrite_s3"])

        out_dir = config["OUT_DIR"]
        bb_file = config["alt_bb_file"]
        bb_bucket = config["alt_bb_bucket"]
        meta_dir = config["META_DIR"]
        bucket_out = config["s3_bucket_out"]
        overwrite_s3_flag = config["overwrite_s3"]

        bb_fn = bb_file.split('/')[-1]
        fn = file.split('/')[-1]

        # check to see if output file exists, and decide to process based on overwrite_s3_flag
        parquet_exists = s3_file_exists(bucket_out, out_dir + '/' + fn + ".parquet")

        #print("junk file test, "+str(s3_file_exists(bucket_out, out_dir + '/' + "junk.txt")))
        if parquet_exists:
            if overwrite_s3_flag:
                print("output parquet file already exists, overwriting file ", fn)
            else:
                print("output parquet file already exists, skipping file ", fn)
                return

        # download s3 file to /tmp storage
        try:
            s3.Bucket(bb_bucket).download_file(bb_file, '/tmp/'+ bb_fn)
        except botocore.exceptions.ClientError as e:
            print("Error reading the s3 object " + bb_file)
            exit(-1)
        bright_band = read_alt_bb_file('/tmp/'+ bb_fn)

        print('processing file: ' + file)
        # download s3 file to /tmp storage
        try:
            s3.Bucket(bucket).download_file(file, '/tmp/'+ fn)
        except botocore.exceptions.ClientError as e:
            print("Error reading the s3 object " + file)
            exit(-1)

        have_mrms, outputJson = process_file('/tmp/'+ fn, bright_band)

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
        parquet_output_file = '/tmp/' + fn + '.parquet'

        json2parquet.write_parquet(parquet_data, parquet_output_file, compression='snappy')
        # upload parquet file to s3
        print("uploading parquet VN file "+fn + ".parquet")
        try:
            s3.Bucket(bucket_out).upload_file("/tmp/" + fn + ".parquet", out_dir + '/' + fn + ".parquet")
        except botocore.exceptions.ClientError as e:
            print("Error uploading the s3 object " + out_dir + '/' + fn + ".parquet")
            exit(-1)

        metadata = { "site":outputJson[0]["GR_site"],"vn_filename":outputJson[0]["vn_filename"],
                     "time": outputJson[0]["time"],"site_rainy_count": outputJson[0]["site_rainy_count"],
                     "site_fp_count": outputJson[0]["site_fp_count"],
                     "site_percent_rainy":outputJson[0]["site_percent_rainy"],
                     "meanBB":outputJson[0]["meanBB"],"have_mrms":have_mrms}

        with open('/tmp/'+fn+'.meta.json', 'w') as json_file:
            json.dump(metadata, json_file)
        json_file.close()
        # upload metadata file to s3
        print("uploading metadata file "+fn + ".meta.json")
        try:
            s3.Bucket(bucket_out).upload_file("/tmp/" + fn + ".meta.json", meta_dir + '/' + fn + ".meta.json")
        except botocore.exceptions.ClientError as e:
            print("Error uploading the s3 object " + meta_dir + '/' + fn + ".meta.json")
            exit(-1)

