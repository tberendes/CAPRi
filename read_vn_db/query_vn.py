# ---------------------------------------------------------------------------------------------
#
#  query_vn.py
#
#  Description: this script queries data from the VN database on AWS and downloads a CSV
#               result file, then parses out the values into a dictionary.  Optional saving of
#               the CSV file is supported
#
#  Syntax: currently no input parameters
#
#  To Do: modify to accept input parameters
#
# ---------------------------------------------------------------------------------------------

# --Do all the necessary imports
import csv
import gzip
import os
import shutil
import string
import sys
import time
import urllib
import datetime
import tempfile

import requests

# these are the API endpoints for the query submission and retrieval of results
# they are experimental, and subject to change
url_query = 'https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/test3/'
url_result = 'https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/test3/result/'

max_retry_count = 30000
retry_interval = 2
result_page_size = 3000

import json

class VNQuery:
    def __init__(self):
        self.params = {}
        self.query_id=''
        self.q_params={}
        self.result_url=''
        self.csv_filename=''
        self.result_downloaded = False
        self.temp_file_flag = False
        self.min_list=[]
        self.max_list=[]
        self.diff_list=[]

    def __del__(self):
        # Destructor: remove temporary file
        if self.temp_file_flag and os.path.exists(self.csv_filename):
            os.remove(self.csv_filename)

    # convert str to float if it is a viable numeric value, otherwise do nothing (on exception)
    def str_to_float(self, s):
        try:
            s=float(s)
        except ValueError:
            pass
        return s

    def get_params(self):
        return self.params

    def make_comma_list(self,strlist):
        comma_list=''
        for element in strlist:
            if len(comma_list) >0:
                comma_list=comma_list+','+element
            else:
                comma_list=element
        return comma_list
    def finalize_params(self):
        # add in range and difference parameters
        min_param = self.make_comma_list(self.min_list)
        if len(min_param) >0:
            self.params['min']=min_param
        max_param = self.make_comma_list(self.max_list)
        if len(max_param) >0:
            self.params['max']=max_param
        diff_param = self.make_comma_list(self.diff_list)
        if len(diff_param) >0:
            self.params['difference']=diff_param

    def submit_query(self):
        # delete previous query temporary CSV file is present
        if self.temp_file_flag and self.result_downloaded:
            self.delete_csv()
        self.result_downloaded = False
        self.finalize_params()
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

    def download_csv(self, **kwargs):
        # file_name = self.result_url.split("/")[-1]
        f = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
        self.csv_filename = f.name
        self.temp_file_flag = True
        for key, value in kwargs.items():
            if key == 'filename':
                self.csv_filename = value
                self.temp_file_flag = False
                f = open(self.csv_filename, 'w+b')
        res = self.wait_for_query()
        if res['status'] != 'success':
            return {'status': 'failed', 'message': 'CSV download failed'}

        get_response = requests.get(self.result_url, stream=True)
        #        with open(file_name, 'wb') as f:
        for chunk in get_response.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
        print("fname ", self.csv_filename)
        f.close()
        self.result_downloaded = True

        return {'status':'success', 'message':'Successfully downloaded CSV results'}

    def save_csv(self, filename):
        if os.path.exists(self.csv_filename):
            shutil.copy(self.csv_filename, filename)
        else:
            print("Can not copy temporary file ", self.csv_filename, " to ", filename)
            return {'status': 'failed', 'message': 'Failed to save CSV results'}
        return {'status': 'success', 'message': 'Successfully saved CSV results'}

    def delete_csv(self):
        if os.path.exists(self.csv_filename):
            os.remove(self.csv_filename)
        else:
            print("Can not delete temporary file ", self.csv_filename)

    def get_csv(self):
        # check for downloaded results file
        if not self.result_downloaded:
            res = self.download_csv()
            if res['status'] != 'success':
                return res
        # read downloaded results
        matchupDict = {}

        row_cnt = 0
        with open(self.csv_filename) as f:
            # parse CSV file
            for row in csv.DictReader(f):
                for key, value in row.items():
                    if key not in matchupDict:
                        matchupDict[key] = []
                    matchupDict[key].append(self.str_to_float(value))
                row_cnt = row_cnt + 1
        f.close()

        print("read ", row_cnt, " records")

        return {'status':'success', 'message':'Successfully retrieved CSV results', 'results':matchupDict}

    # add a single key,value pair as a filter parameter
    def add_parameter(self, key, value):
        self.params[key] = value

    # convenience functions for common filtering methods
    def set_columns(self, comma_list):
        self.params['columns'] = comma_list
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

    def add_min_filter(self,variable,min):
        self.min_list.append(variable)
        self.min_list.append(str(min))
    def add_max_filter(self,variable,max):
        self.max_list.append(variable)
        self.max_list.append(str(max))
    def add_range_filter(self,variable,min,max):
        self.add_min_filter(variable,min)
        self.add_max_filter(variable,max)
    def add_difference_threshold_filter(self,variable1, variable2, relation, value):
        self.diff_list.append(variable1)
        self.diff_list.append(relation)
        self.diff_list.append(variable2)
        self.diff_list.append(str(value))
        #from Pooja:
        #difference = col1, comparator, col2, value, col2, comparator, col4, value
        #I have made a group of 4, each  with col1,comparator,col2,value which generates
        # col1 - col2 comparator(<,>,>=<,>=) value. The comparators mapped  are
        # {“gte”:“>=“, “lte”:“<=“, “lt”: “<”, “gt”: “>”, “eq”: “=”} . For example :
        # difference= topheight,lt,ruc_0_height,4,bottomheight,gte,ruc_0_height,5,hid1,eq,hid2,6 will
        # generate clauses topheight - ruc_0_height < 4 AND bottomheight - ruc_0_height >= 5 AND hid1 - hid2 = 6

def main():

    ts = datetime.datetime.now().timestamp()
    print("start time: ", ts)

#    params = {'start_time': "2019-03-21 00:00:00", 'end_time': "2019-04-21 00:00:00"}
    # initialize query class
    query = VNQuery()

    # initialize query parameters
    query.set_time_range("2019-03-21 00:00:00", "2019-03-24 00:00:00")
    query.set_columns("time,latitude,longitude,GR_Z,Dm,gr_site,vn_filename,raynum,scannum,elev")
    #Various available filter methods, can use in any combination:
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

    #Here’s what I have in mind for this…specify the parameter ‘topheight_below_ruc_0’=[some_value]
    # and it queries the ruc_0_height column and topHeight column and returns the results
    # where topHeight-ruc_0_height < [some_value]. Similarly for the bottomHeight…’bottomheight_above_ruc_0’=[some_value]
    # which queries the ruc_0_height and bottomHeight columns and returns results where bottomHeight-ruc_0_height > [some_value].

    # can use 'lt', 'lte', 'gt', 'gte', 'eq' for relation
    query.add_difference_threshold_filter('topHeight', 'ruc_0_height', 'lt', 1)
    #query.add_difference_threshold_filter('bottomHeight', 'ruc_0_height', 'gt', 1)

    #query.add_range_filter('ruc_0_height', 0.0, 2.0)

    # submit query to AWS
    res = query.submit_query()
    if res['status'] != 'success':
        print("Query failed: ", res['message'])
        exit(-1)

    # download csv file, may specify optional filename
    # if optional filename is ommitted, uses temporary file
    # which is automatically deleted on exit of program
    # check 'status' entry for 'success' or 'failed'
    res = query.download_csv(filename="test_csv.csv")
    if res['status'] != 'success':
        print("Download failed: ", res['message'])
        exit(-1)

    # download (if not already downloaded) and read CSV file and return dictionary with status and results
    result = query.get_csv()
    if result['status'] != 'success':
        print("Get results failed: ", result['message'])
        exit(-1)

    if 'status' not in result or result['status'] == 'failed':
        print("Query failed")
        exit(-1)
    if 'results' in result:
        # extract matchups dictionary from result dictionary
        matchups = result['results']
    else:
        print("Query found no matchups")
        exit(0)

    if len(matchups)==0:
        print("Query found no matchups")
        exit(0)

    # examples of manipulating and processing matchup results

    # get list of unique sites present in the results
    gr_sites = {}
    for site in matchups["gr_site"]:
        gr_sites[site]="found"

    if len(gr_sites.keys()) > 0:
        print("Radar sites present in query return:")
        print(gr_sites.keys())

    # print number of results in dictionary, pick a field and count entries
    if 'latitude' in matchups:
        num_results = len(matchups['latitude']) # pick a key value to get count of results
        print("number of VN volume matches: ", num_results)

    # print first and last matchup values
    # for key,values in matchups.items():
    #     print("key ", key, " value[0] ", values[0])
    # for key,values in matchups.items():
    #     print("key ", key, " value[-1] ", values[-1])

    # Example: create unique sort order field by combining filename, and volume parameters
    # create index sorted by vn_filename, raynum, scannum, elev
    sort_dict = {}
    fnames = matchups['vn_filename']
    raynum = matchups['raynum']
    scannum = matchups['scannum']
    elev = matchups['elev']

    # create index dictionary for sorting
    for cnt in range(len(fnames)):
        sort_str = fnames[cnt]+str(raynum[cnt])+str(scannum[cnt])+str(elev[cnt])
#        print("cnt ", cnt, " sort str ", sort_str)
        sort_dict[sort_str] = cnt

    # sort by filename
    sorted_index = []
    for fname in sorted(sort_dict):
        sorted_index.append(sort_dict[fname])

    file1 = open("sort_test.txt", "w")  # write mode
    for i in range(len(fnames)):
        #print(fnames[sorted_index[i]], ",", raynum[sorted_index[i]], ","
        #      , scannum[sorted_index[i]], ",", elev[sorted_index[i]])
        print(fnames[sorted_index[i]], ",", raynum[sorted_index[i]], ","
              , scannum[sorted_index[i]], ",", elev[sorted_index[i]], file=file1)
    file1.close()

    # elapsed time for metrics
    endts = datetime.datetime.now().timestamp()
    print("end time: ", endts)

    diff = endts - ts
    print("elapsed time ", diff, "secs")

    # delete CSV file if done with it, only necessary if you supplied a filename to query.download_csv()
    # query.delete_csv()

if __name__ == '__main__':
   main()
