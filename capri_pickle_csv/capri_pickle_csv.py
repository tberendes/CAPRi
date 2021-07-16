# ---------------------------------------------------------------------------------------------
#
#  capri_pickle_csv.py
#
#  Author:  Todd Berendes, UAH ITSC, July 2021
#
#  Description: This program reads a csv file into a dictionary and saves it as a binary "pickle" file.
#               The .pcl file corresponds to the Bright Band file used in the capri_vn_to_db.py program.
#  Syntax: currently no input parameters
#
# ---------------------------------------------------------------------------------------------
import sys
from urllib.parse import unquote_plus, urlparse, urljoin
import pickle
import boto3
import botocore
import csv

s3 = boto3.resource(
    's3')

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

def load_bb_from_s3(bucket, key,fn):
    #print("load_bb_from_s3 file key " + key)
    filename = "/tmp/" + fn

    try:
        bucket.download_file(key, filename)
    except botocore.exceptions.ClientError as e:
        print("Error reading the s3 object " + key)
        jsonData = {"message": "error"}
        return jsonData
    return read_alt_bb_file(filename)

def lambda_handler(event, context):

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        fn = key.split('/')[-1]
        print('fn key '+key)
        print('fn split '+fn)

#        input_json = load_json(bucket, key)
        alt_bb_dict = load_bb_from_s3(s3.Bucket(bucket), key,fn)
        if "message" in alt_bb_dict and alt_bb_dict["message"] == "error":
            print("error loading BB file, exiting...")
            sys.exit(1)

        print("pickling BB file " + key)
        f = open("/tmp/" + fn + ".pcl", "wb")
        pickle.dump(alt_bb_dict, f)
        f.close()
        s3.Bucket(bucket).upload_file("/tmp/" + fn + ".pcl", key + ".pcl")

        #print("test read of pickle file...")
        #alt_bb_dict_test = load_bb_from_s3(s3.Bucket(bucket), key + ".pcl", fn + ".pcl")

