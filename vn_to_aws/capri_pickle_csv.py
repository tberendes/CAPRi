import sys
from urllib.parse import unquote_plus, urlparse, urljoin
import pickle
import boto3
import botocore

from extract_vn import read_alt_bb_file

s3 = boto3.resource(
    's3')

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

