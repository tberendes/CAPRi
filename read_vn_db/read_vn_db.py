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
import csv
import gzip
import os
import string
import sys
import time
import urllib
import datetime
import tempfile

import requests

url_query = 'https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/test3/'
url_result = 'https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/test3/result/'

max_retry_count = 30
retry_interval = 2
result_page_size = 3000

import json

class VNQuery:
    def __init__(self):
        self.params = {}
        self.query_id=''
        self.q_params={}
        self.result_url=''
    def get_params(self):
        return self.params
    def submit_query(self):
        try:
            response = requests.get(url_query, params=self.params)

            # If the response was successful, no Exception will be raised
            response.raise_for_status()
        except requests.HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')  # Python 3.6
            return {'status': 'failed', 'message': 'HTTP error occurred during query submission'}
        except Exception as err:
            print(f'Other error occurred: {err}')  # Python 3.6
            return {'status': 'failed', 'message': 'Error occurred during query submission'}
        #        else:
        print('Successfully connected with server, submitted query ...')

        print(response.text)
        self.query_id = response.text.split('queryId=')[1].split('}')[0]

        print(self.query_id)
        self.q_params = {'qid': self.query_id}

        return {'status': 'success', 'message': 'Query successfully submitted'}

    def wait_for_query(self):
        # From Pooja:  Hi Todd, I have implemented pagination and also the status (Running, queued, cancelled, failed, succeeded, error)
        # for the results. The api returns 100 items by default but you can configure that using page_size parameter
        # in the url. For the pagination, I have used page_token parameter. The page_token parameter for next page
        # is given in the result json.
        status = 'waiting'
        retry_count = 0
        print('Waiting for results...')
        # set page size to zero for status only
        params = self.q_params
#        params['page_size'] = 0
        while True:
            # loop until result is ready or an error has occurred
            try:
                response = requests.get(url_result, params=self.q_params)

                # If the response was successful, no Exception will be raised
                response.raise_for_status()
            except requests.HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Python 3.6
            except Exception as err:
                print(f'Other error occurred: {err}')  # Python 3.6
            #        else:

            r = response.json()
            # print("r ",r)
            if 'status' in r:
                status = r['status']
            if str(status).lower() == 'succeeded':
                break
            if str(status).lower() == 'failed' or str(status).lower() == 'error':
                return {'status': 'failed', 'message': str(status).lower()}
            print("query status: ", str(status).lower())
            time.sleep(retry_interval)
            retry_count = retry_count + 1
            if retry_count > max_retry_count:
                print('HTTP Error: Exceeded maximum retries')
                return {'status': 'failed', 'message': 'HTTP Error: Exceeded maximum retries'}

        if 'file_url' in r:
            self.result_url = r['file_url']
        print("Athena result URL: ", self.result_url)

        return {'status': 'success', 'message': 'Successfully performed query'}

    def get_results(self):

        q_res = self.submit_query()
        if q_res['status'] != 'success':
            return {'status': 'failed', 'message': 'Query submission failed'}
        w_res = self.wait_for_query()
        if w_res['status'] != 'success':
            return {'status': 'failed', 'message': 'Query execution failed'}
        # need to do pagination of results
        matchupDict = {}
    #    matchupList = []
        offset='0'
        page_token = ''
        page_count = 1
        result_cnt = 0
        while True:
            # sleep until result is ready or an error has occurred
            q_params = {'qid': self.query_id, 'page_size':result_page_size}
            if len(page_token) > 0:
                print("page token ", page_token)
    #            token=str(urllib.parse.quote(page_token,safe=''))
                q_params['page_token']=page_token

            # loop until result is ready or an error has occurred
            try:
                response = requests.get(url_result, params=q_params)
                print("request: ", response.request.url)
                # If the response was successful, no Exception will be raised
                response.raise_for_status()
            except requests.HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Python 3.6
            except Exception as err:
                print(f'Other error occurred: {err}')  # Python 3.6

            #r = requests.get(url_result, params=q_params)
            #print(r)
            result = response.json()
            #result = json.loads(r.text)
            #print(result)
            #print(data['result'])
            # Process the payload or add it to a list
            if str(result['status']).lower() == 'error':
                print("Error: ", result['message'])
                print("Query parameters used: ", q_params)
                return {'status': 'failed', 'message': 'bad query'}

            #print("result: ", result)
            for entry in result['data']:
    #            matchupList.append(entry)
                for key,value in entry.items():
                    if key not in matchupDict:
                        matchupDict[key] = []
                    matchupDict[key].append(value)
                result_cnt = result_cnt+1

                #print("entry: ", entry)
            if 'page_token' in result:
                page_token = result['page_token']
            else:
                break
            page_count = page_count + 1

        print("processed ", page_count, " pages")
        print("read ", result_cnt, " records")
        return {'status':'success', 'message':'success', 'results':matchupDict}

    def download_csv(self):
        q_res = self.submit_query()
        if q_res['status'] != 'success':
            return {'status': 'failed', 'message': 'Query submission failed'}
        w_res = self.wait_for_query()
        if w_res['status'] != 'success':
            return {'status': 'failed', 'message': 'Query execution failed'}

        get_response = requests.get(self.result_url, stream=True)
        #file_name = self.result_url.split("/")[-1]
        f = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
#        with open(file_name, 'wb') as f:
        for chunk in get_response.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
        fname = f.name
        print("fname ",fname)
        f.close()
        #f.seek(0)
        matchupDict = {}

        row_cnt = 0
        # with open(fname, "r") as f:
        #     reader = csv.reader(f)
        #     keys=[]
        #     for line in reader:
        #         if row_cnt == 0:
        #             for key in line:
        #                 keys.append(key)
        #         else:
        #             for i in range(len(keys)):
        #                 if keys[i] not in matchupDict:
        #                     matchupDict[keys[i]] = []
        #                 matchupDict[keys[i]].append(line[i])
        #         row_cnt = row_cnt + 1
        #     row_cnt = row_cnt - 1 # subtract one for header line
        # f.close()

        with open(fname) as f:
            # parse CSV file
            for row in csv.DictReader(f):
                for key, value in row.items():
                    if key not in matchupDict:
                        matchupDict[key] = []
                    matchupDict[key].append(value)
                row_cnt = row_cnt + 1
        f.close()

        if os.path.exists(fname):
            os.remove(fname)
        else:
            print("Can not delete temporary file ", fname)
        print("read ", row_cnt, " records")

        return {'status':'success', 'message':'Successfully downloaded CSV results', 'results':matchupDict}

    # add a single key,value pair as a filter parameter
    def add_parameter(self, key, value):
        self.params[key] = value

    # convenience functions for common filtering methods
    def set_time_range(self, start_time, end_time):
        self.params['start_time'] = start_time
        self.params['end_time'] = end_time
    def set_lat_lon_box(self, start_lat, end_lat, start_lon, end_lon):
        self.params['start_lat'] = start_lat
        self.params['end_lat'] = end_lat
        self.params['start_lon'] = start_lon
        self.params['end_lon'] = end_lon
    #start_ray and end_ray are zero based and inclusive i.e. ray >= start_ray AND ray <= end_ray
    def set_inner_swath(self, start_ray, end_ray):
        self.params['start_ray_num'] = start_ray
        self.params['end_ray_num'] = end_ray
        self.params['swath'] = "inner"
    #start_ray and end_ray are zero based and exclusive i.e. ray < start_ray OR ray > end_ray
    def set_outer_swath(self, start_ray, end_ray):
        self.params['start_ray_num'] = start_ray
        self.params['end_ray_num'] = end_ray
        self.params['swath'] = "outer"
    def set_gpm_version(self,version):
        self.params['gpm_ver_like'] = version
    def set_vn_version(self,version):
        self.params['vn_ver_like'] = version
    #NS, MS, HS, FS
    def set_scan(self,type):
        self.params['scan_like'] = type
    def set_sensor(self,type):
        self.params['sensor_like'] = type
    # can also use % as a wildcard, i.e. site="K%" and list of sites i.e. site="KI%,KM%,KF%"
    def set_gr_site(self,site):
        self.params['gr_site_like'] = site
    def set_gr_site_exclude(self,site):
        self.params['gr_site_not_like'] = site
    def set_zfact_measured_range(self,min,max):
        self.params['min_zfact_measured'] = min
        self.params['max_zfact_measured'] = max
    def set_zfact_corrected_range(self,min,max):
        self.params['min_zfact_corrected'] = min
        self.params['max_zfact_corrected'] = max
    def set_grz_range(self,min,max):
        self.params['min_grz'] = min
        self.params['max_grz'] = max
    def set_dm_range(self,min,max):
        self.params['min_dm'] = min
        self.params['max_dm'] = max
    def set_gr_dm_range(self,min,max):
        self.params['min_gr_dm'] = min
        self.params['max_gr_dm'] = max
    def set_site_percent_rainy_range(self,min,max):
        self.params['min_site_percent_rainy'] = min
        self.params['max_site_percent_rainy'] = max
    def set_site_fp_count_range(self,min,max):
        self.params['min_site_fp_count'] = min
        self.params['max_site_fp_count'] = max

def main():

    ts = datetime.datetime.now().timestamp()
    print("start time: ", ts)

#    params = {'start_time': "2019-03-21 00:00:00", 'end_time': "2019-04-21 00:00:00"}
    query = VNQuery() # initialize query parameters
    query.set_time_range("2019-03-21 00:00:00", "2019-03-24 00:00:00")

    #available filter methods:

    #query.set_gr_site("KFSD")
    # query.set_lat_lon_box(start_lat, end_lat, start_lon, end_lon)
    # query.set_inner_swath(start_ray, end_ray)
    # query.set_inner_swath(start_ray, end_ray)
    # query.set_gpm_version(version)
    # query.set_vn_version(version)
    # query.set_scan(type)
    # query.set_sensor(type)
    # query.set_gr_site(site)
    # query.set_gr_site_exclude(site)
    # query.set_zfact_measured_range(min,max)
    # query.set_zfact_corrected_range(min,max)
    # query.set_grz_range(min,max)
    # query.set_dm_range(min,max)
    # query.set_gr_dm_range(min,max)
    # query.set_site_percent_rainy_range(min,max)
    # query.set_site_fp_count_range(min,max)

    #print(query.get_params())

    #exit(0)

    result = query.get_results()
    #result = query.download_csv()

    if 'status' not in result or result['status'] == 'failed':
        print("Query failed")
        exit(-1)
    if 'results' in result:
        matchups = result['results']
    else:
        print("Query found no matchups")
        exit(0)

    # get list of sites
    gr_sites = {}
    for site in matchups["gr_site"]:
        gr_sites[site]="found"

    if len(gr_sites.keys()) > 0:
        print("Radar sites present in query return:")
        print(gr_sites.keys())

    # print number of results in dictionary
    if 'results' in result:
        num_results = len(matchups['latitude']) # pick a key value to get count of results
        print("number of VN volume matches: ", num_results)

    # for key,values in matchups.items():
    #     print("key ", key, " value[0] ", values[0])
    # for key,values in matchups.items():
    #     print("key ", key, " value[-1] ", values[-1])
    endts = datetime.datetime.now().timestamp()
    print("end time: ", endts)

    #for validation experiment, create unique sort order field by combining filename, and volume parameters
    # create index sorted by vn_filename, raynum, scannum, elev
    sort_dict = {}
    fnames = matchups['vn_filename']
    raynum = matchups['raynum']
    scannum = matchups['scannum']
    elev = matchups['elev']

    # create index dictionary
    for cnt in range(len(fnames)):
        sort_str = fnames[cnt]+raynum[cnt]+scannum[cnt]+elev[cnt]
#        print("cnt ", cnt, " sort str ", sort_str)
        sort_dict[sort_str] = cnt

    # sort by filename
    sorted_index = []
    for fname in sorted(sort_dict):
        sorted_index.append(sort_dict[fname])

    file1 = open("stream_test.txt", "w")  # write mode
#    file1 = open("csv_file_test_dict.txt", "w")  # write mode
#    file1 = open("csv_file_test_reader.txt", "w")  # write mode

    for i in range(len(fnames)):
        print(fnames[sorted_index[i]], ",", raynum[sorted_index[i]], ","
              , scannum[sorted_index[i]], ",", elev[sorted_index[i]])
        print(fnames[sorted_index[i]], ",", raynum[sorted_index[i]], ","
              , scannum[sorted_index[i]], ",", elev[sorted_index[i]], file=file1)

#     for key,values in matchups.items():
#         for i in range(len(values)):
#             print(i, " ", key, " ", values[sorted_index[i]])
#             print(i, " ", key, " ", values[sorted_index[i]], file=file1)
    file1.close()

    diff = endts - ts
    print("elapsed time ", diff, "secs")

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


# API Endpoint
# /?start_time="2014-03-21 03:51:26"&end_time= "2020-03-21 03:51:26"
# /?start_lat=19&end_lat=64&start_lon=-161&end_lon=-68
# /?start_ray_num=2&end_ray_num=8&swath="outer"
# /?start_ray_num=2&end_ray_num=8
# /?start_ray_num=2&end_ray_num=8&swath="inner"
# /?gpm_ver_like="V06A"
# /?vn_ver_like=1.21
# /?scan_like=NS
# /?sensor_like=DPR
# /?gr_site_like=K%
# /?gr_site_like=%R
# /?gr_site_like=%AB%
# /?gr_site_not_like=K%
# /?gr_site_like=KI%,KM%,KF%
# /?gr_site_not_like=KI%,KM%,KF%
# gr_site_not_like=KI%,KM%,KF%&gr_site_like=%J,%C
# /?vn_filename_like=%KARX%
# /?min_zfact_measured=20&max_zfact_measured=40
# /?min_zfact_corrected=20&max_zfact_corrected=40
#  /?min_grz=20&max_grz=40
# /?min_dm=1.99&max_dm=2.01
# /?min_gr_dm=0.99&max_gr_dm=1.01
# /?min_site_percent_rainy=6&max_site_percent_rainy=7
# /?min_site_fp_count=600&max_site_fp_count=700
# /?start_ray_num=2&end_ray_num=8&swath="inner"&sensor_not_like=k,l
# /?start_time="2014-01-01 00:00"&end_time="2020-01-01 59:00"&scan_not_like="K%"
# /?start_lat=45&end_lon=86&scan_not_like="K%"
# /?start_ray_num=2&end_ray_num=8&swath="inner"&sensor_not_like=%k,l%&scan_like=K%,N%
# start_ray_num=2&end_ray_num=8&swath="outer"&sensor_not_like=%k,l%&scan_like=K%,N%
# /
