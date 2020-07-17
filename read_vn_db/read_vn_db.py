# ---------------------------------------------------------------------------------------------
#
#  read_vn_db.py
#
#  Description: this script queries data from the VN database on AWS and
#               and parses out the values and formats output for JSON
#
#  Syntax: currently no input parameters
#
#  To Do: modify to accept input parameters, such as filename and maybe the location of district coordinates
#
# ---------------------------------------------------------------------------------------------


# --Do all the necessary imports
import gzip
import string
import sys
import time

import requests

url_query = 'https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/dev/'
url_result = 'https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/dev/result'

import json

def get_http_data(arg_dict):

    r = requests.get(url_query,params=arg_dict)
    print(r.text)
    query_id = r.text.split('queryId=')[1].split('}')[0]

    print(query_id)
    p_dict = {'qid': query_id}

# HACK!
# need a status query for result, results aren't ready, query returns an error
# sleep for 5 seconds to give results time to accumulate
    time.sleep(2)

    # need to do pagination of results
    matchupList = []
    offset='0'
    while True:
        r = requests.get(url_result+'/?offset='+offset, params=p_dict)
        #print(r)
        data = json.loads(r.text)
        print(data)
        #print(data['result'])
        # Process the payload or add it to a list
        for entry in data['result']:
            matchupList.append(entry)
            #print("entry: ", entry)

        # offset = data['offset'] # offset +1?
        # print("offset "+ offset)
        # hasMore = data['has-more']
        # print("has-more "+hasMore)
        # if not hasMore:
        break

    return matchupList

def get_http_data_test(arg_dict):

    r = requests.get(url_query,params=arg_dict)
    print(r.text)
    query_id = r.text.split('queryId=')[1].split('}')[0]

    print(query_id)
    p_dict = {'qid': query_id}

# HACK!
# need a status query for result, results aren't ready, query returns an error
# sleep for 5 seconds to give results time to accumulate
    time.sleep(2)

    # need to do pagination of results
    s = requests.Session()
    matchupList = []
    response = s.get(url_result, params=p_dict)
    print(response)
    resp_json = response.json()
    print(json.dumps(resp_json))
    matchupList.append(resp_json)

    while resp_json.get('hasMore') == True:
        print("page...")
        response = s.get(resp_json['nextPageUrl'])
        resp_json = response.json()
        matchupList.append(resp_json)

#    result = requests.get(url_result, params=p_dict)

    # success=False
    # count = 0
    # while not success and count < 5:
    #     result = requests.get(url_result,params=p_dict)
    #     print(result)
    #     if result['success']:
    #         success=True
    #     else:
    #         time.sleep(5)
    #     count = count + 1
    #     print(count)
    #result = requests.get(url_result+"?qid="+query_id)

    #return result
    return matchupList


def main():
    print, "in main"

    params = {'start_time': "2019-03-21 00:00:00", 'end_time': "2019-04-22 00:00:00"}
    result = get_http_data(params)

    for entry in result:
        print(entry)

if __name__ == '__main__':
   main()

# API’s to trigger the lambdas:
# Eg: For getting average precipitation over total data,
# 1. https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/dev/?inputQueryString
# =avg(preciprate)
# Output:
# { "success" : true,
# "queryID" : "0b392dd6-2478-474d-9e63-a22beaa71fe4" }
# Give the Resultant Query Execution Id to the below URL
#
# 2. https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/dev/result?qid=<qid>
# Eg:
# https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/dev/result?qid=0b392
# dd6-2478-474d-9e63-a22beaa71fe4
# Output:
# {"Status": "Success", "Result": [{"_col0":
# "1.9659063383908282"}]}
#
# 3. Athena:
# Crawler: capri_parquet
# Source Data S3 bucket: capri-data/parquet
# Result Data S3 bucket: aws-athena-query-results-capri-real-time
# Database: capri_real_time_query
# Table: parquet
# Corresponding SQL for the above API :
# SELECT AVG(preciprate) FROM "capri_real_time_query"."parquet";

# start_time	time	Parameters which will be queried by their minimum and maximum range
# end_time	time
# start_lat	Latitude
# end_lat	Latitude
# start_lon	Longitude
# end_lon	Longitude
# min_zfact_measured	zfactormeasured
# max_zfact_measured	zfactormeasured
# min_zfact_corrected	zfactorcorrected
# max_zfact_corrected	zfactorcorrected
# min_grz	gr_z
# max_grz	gr_z
# min_dm	dm
# max_dm	dm
# min_gr_dm	gr_dm
# max_gr_dm	gr_dm
# min_site_percent_rainy	site_percent_rainy
# max_site_percent_rainy	site_percent_rainy
# min_site_fp_count	site_fp_count
# max_site_fp_count	site_fp_count
# start_ray_num	raynum	Parameters which will be queried to have either outer coverage range or inner coverage range , Assumption : For the below parameters , there exists another swath parameter, for eg: swath="outer" or swath = "inner" for rayNum
# end_ray_num	raynum
# scan_like	scan	Parameters which will be queried by the presence/absence of some strings/substrings
# scan_not_like	scan
# gr_site_like	gr_site
# gr_site_not_like	gr_site
# vn_filename_like	vn_filename
# vn_filename_not_like	vn_filename
# gpm_ver_like	gpm_ver
# gpm_ver_not_like	gpm_ver
# vn_ver_like	vn_ver
# vn_ver_not_like	vn_ ver
# sensor_like	sensor
# sensor_not_like	sensor