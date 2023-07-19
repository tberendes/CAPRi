# ---------------------------------------------------------------------------------------------
#
#  read_vn.py
#
#  Description: This program processes local subdirectories of GPM VN files, formatted in netCDF
#               and parses out the values and formats output for JSON and uploads to s3 bucket.
#               This is a stand-alone version the VN to Athena conversion process that produces
#               local output and can upload to S3 using Boto with credentials
#
#  Syntax: currently no input parameters
#
#  To Do: modify to accept input parameters, such as filename and maybe the location of district coordinates
#
# ---------------------------------------------------------------------------------------------


# --Do all the necessary imports

import gzip
import logging
import sys
from time import sleep
import os

import boto3 as boto3
import json2parquet
from botocore.exceptions import ClientError

import json

#s3 = boto3.resource(
#    's3')
from extract_vn import process_file

session = boto3.Session(profile_name='CAPRI')
# Any clients created from this session will use credentials
# from the [CAPRI] section of ~/.aws/credentials.
client = session.client('s3')

num_retries = 10
sleep_secs = 20


#    return json.dumps(districtPrecipStats)
def upload_file(local_file, s3_bucket, s3_key):
    upload=False
    for i in range(num_retries):
        try:
            response = client.upload_file(local_file, s3_bucket, s3_key)
            upload=True
            break
        except ClientError as e:
            logging.error(e)
            print("Error during file upload " + s3_key + " to S3 bucket " + s3_bucket + " ...")
            print(e)
            sleep(sleep_secs)
            print("retry ", i,'/',num_retries)
    return upload

def upload_s3(local_file, s3_bucket, s3_key, overwrite):
    success = False
    try:
        # head_object will return exception if file does not already exist
        client.head_object(Bucket=s3_bucket, Key=s3_key)
        if overwrite:
            print("file " + s3_key + " is already in S3 bucket " + s3_bucket + ", Overwriting ...")
            #client.upload_file(local_file, s3_bucket, s3_key)
            success = upload_file(local_file, s3_bucket, s3_key)
        else:
            print("file " + s3_key + " is already in S3 bucket " + s3_bucket + ", Skipping ...")
    except:
        print("Uploading " + s3_key + " to S3 bucket " + s3_bucket + " ...")
        #client.upload_file(local_file, s3_bucket, s3_key)
        success = upload_file(local_file, s3_bucket, s3_key)
    if not success:
        print("Fatal error: Could not upload " + s3_key + " to S3 bucket " + s3_bucket )
    return success

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

    # TODO: need to handle missing alt_bb file, skip if not specified

    # support single file processing, if VN_FILE is defined, only process single file
    #config['VN_FILE'] = 'filename'
    # dirwalk=[]
    # if config.has_key('VN_FILE'):
    #     dirwalk.append((config['VN_DIR'],[],config['VN_FILE'])) # append as tuple
    # else:
    #     for root, dirs, files in os.walk(config['VN_DIR'], topdown=False):
    #         dirwalk.append((root, dirs, files)) # append as tuple
    #for root, dirs, files in dirwalk:

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

                    have_mrms, outputJson = process_file(os.path.join(root,file))
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
                                 "meanBB":outputJson[0]["meanBB"],
                                 "have_mrms":have_mrms}
                    os.makedirs(os.path.join(config['META_DIR']), exist_ok=True)
                    with open(os.path.join(config['META_DIR'],file+'.meta.json'), 'w') as json_file:
                        json.dump(metadata, json_file)
                    json_file.close()

                    if config['upload_parquet']:
                        #print("uploading parquet "+os.path.join(OUT_DIR,file+'.parquet'))
                        parquet_key = config['s3_parquet_dir']+'/'+file+'.parquet'
                        if not upload_s3(os.path.join(config['OUT_DIR'],file+'.parquet',), config['s3_bucket'], parquet_key,config['overwrite_upload_flag']):
                            exit(-1)

                    if config['upload_meta']:
                        #print("uploading metadata "+os.path.join(META_DIR,file+'.meta.json'))
                        metadata_key = config['s3_meta_dir']+'/'+file+'.meta.json'
                        if not upload_s3(os.path.join(config['META_DIR'],file+'.meta.json'), config['s3_bucket'], metadata_key,config['overwrite_upload_flag']):
                            exit(-1)

                # look for deep leraning training and image files with same base filename
                # put deep learning binary files and images in S3
                if config['upload_bin']:
                    # check for GPM and MRMS DL training files (.bin)
                    if os.path.isfile(os.path.join(root, file + '.gpm.bin')):
                        if not upload_s3(os.path.join(root,file+'.gpm.bin'), config['s3_bucket'], config['s3_bin_dir']+'/'+file+'.gpm.bin',config['overwrite_upload_flag']):
                            exit(-1)
                    if os.path.isfile(os.path.join(root, file + '.mrms.bin')):
                        if not upload_s3(os.path.join(root,file+'.mrms.bin'), config['s3_bucket'], config['s3_bin_dir']+'/'+file+'.mrms.bin',config['overwrite_upload_flag']):
                            exit(-1)
                    if os.path.isfile(os.path.join(root, file + '.fp.bin')):
                        if not upload_s3(os.path.join(root,file+'.fp.bin'), config['s3_bucket'], config['s3_bin_dir']+'/'+file+'.fp.bin',config['overwrite_upload_flag']):
                            exit(-1)

                if config['upload_img']:
                    # check for GPM and MRMS DL images and kml files
                    if os.path.isfile(os.path.join(root, file + '.gpm.bw.png')):
                        if not upload_s3(os.path.join(root,file+'.gpm.bw.png'), config['s3_bucket'], config['s3_img_dir']+'/'+file+'.gpm.bw.png',config['overwrite_upload_flag']):
                            exit(-1)
                    if os.path.isfile(os.path.join(root, file + '.gpm.bw.kml')):
                        if not upload_s3(os.path.join(root,file+'.gpm.bw.kml'), config['s3_bucket'], config['s3_img_dir']+'/'+file+'.gpm.bw.kml',config['overwrite_upload_flag']):
                            exit(-1)
                    if os.path.isfile(os.path.join(root, file + '.gpm.col.png')):
                        if not upload_s3(os.path.join(root,file+'.gpm.col.png'), config['s3_bucket'], config['s3_img_dir']+'/'+file+'.gpm.col.png',config['overwrite_upload_flag']):
                            exit(-1)
                    if os.path.isfile(os.path.join(root, file + '.gpm.col.kml')):
                        if not upload_s3(os.path.join(root,file+'.gpm.col.kml'), config['s3_bucket'], config['s3_img_dir']+'/'+file+'.gpm.col.kml',config['overwrite_upload_flag']):
                            exit(-1)

                    if os.path.isfile(os.path.join(root, file + '.mrms.bw.png')):
                        if not upload_s3(os.path.join(root,file+'.mrms.bw.png'), config['s3_bucket'], config['s3_img_dir']+'/'+file+'.mrms.bw.png',config['overwrite_upload_flag']):
                            exit(-1)
                    if os.path.isfile(os.path.join(root, file + '.mrms.bw.kml')):
                        if not upload_s3(os.path.join(root,file+'.mrms.bw.kml'), config['s3_bucket'], config['s3_img_dir']+'/'+file+'.mrms.bw.kml',config['overwrite_upload_flag']):
                            exit(-1)
                    if os.path.isfile(os.path.join(root, file + '.mrms.col.png')):
                        if not upload_s3(os.path.join(root,file+'.mrms.col.png'), config['s3_bucket'], config['s3_img_dir']+'/'+file+'.mrms.col.png',config['overwrite_upload_flag']):
                            exit(-1)
                    if os.path.isfile(os.path.join(root, file + '.mrms.col.kml')):
                        if not upload_s3(os.path.join(root,file+'.mrms.col.kml'), config['s3_bucket'], config['s3_img_dir']+'/'+file+'.mrms.col.kml',config['overwrite_upload_flag']):
                            exit(-1)

                #sys.exit()

if __name__ == '__main__':
   main()
