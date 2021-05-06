# ---------------------------------------------------------------------------------------------
#
#  vnlib.py
#
#  Author:  Todd Berendes, UAH ITSC, March 2021
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
import os
import shutil
import time
import tempfile
import struct
import numpy as np
import json
import requests

class VNQuery:
    # these are the API endpoints for the query submission and retrieval of results
    # they are experimental, and subject to change
    #old versions:
    # url_query = 'https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/test3/'
    # url_result = 'https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/test3/result/'

    # new version with 'fp' and 'zero_deg_height'
    url_query = 'https://o381wx9rqh.execute-api.us-east-1.amazonaws.com/dev/'
    url_result = 'https://o381wx9rqh.execute-api.us-east-1.amazonaws.com/dev/result'
    url_columns = 'https://3gfy5bwy4b.execute-api.us-east-1.amazonaws.com/test'
    url_columns_result = 'https://3gfy5bwy4b.execute-api.us-east-1.amazonaws.com/test/result/'
    max_retry_count = 30000
    retry_interval = 2

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
        self.typeprecip=-1
        self.bbprox=-1

    def __del__(self):
        # Destructor: remove temporary file
        if self.temp_file_flag and os.path.exists(self.csv_filename):
            os.remove(self.csv_filename)
    # add functions to override defaults
    def set_query_url(self,url):
        self.url_query = url
    def set_result_url(self,url):
        self.url_result = url

    # convert str to float if it is a viable numeric value, otherwise do nothing (on exception)
    def str_to_value(self, s):
        try:
            s=int(s)
        except ValueError:
            try:
                s = float(s)
            except ValueError:
                pass
        return s

    def get_params(self):
        return self.params
    def get_params_json(self):
        return json.dumps(self.params)

    def make_comma_list(self,strlist):
        comma_list=''
        for element in strlist:
            if len(comma_list) >0:
                comma_list=comma_list+','+str(element)
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
        # delete previous query temporary CSV file if present
        if self.temp_file_flag and self.result_downloaded:
            self.delete_csv()
        self.result_downloaded = False
        self.finalize_params()
        #print("min_list", self.min_list)
        #print("max_list", self.max_list)
        #print("diff_list", self.diff_list)
        #print("params", self.params)
        try:
            #response = requests.get(url_query, params=self.params)
            response = requests.post(self.url_query, self.get_params_json())

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
        #self.query_id = response.text.split('queryId=')[1].split('}')[0]
        resp_json = json.loads(response.text)
        #self.query_id = response.text.split('queryId:')[1].split('}')[0]
        self.query_id = resp_json['queryId']

        print(self.query_id)
        self.q_params = {'qid': self.query_id}

        return {'status': 'success', 'message': 'Query successfully submitted'}

    def result_loop(self, url, params):
        status = 'None'
        #        params['page_size'] = 0
        retry_count = 0
        while True:
            # loop until result is ready or an error has occurred
            try:
                response = requests.get(url, params=params)

                # If the response was successful, no Exception will be raised
                response.raise_for_status()
            except requests.HTTPError as http_err:
                if response.status_code != 425:  # 425 is returned from lambda if query is in progress
                    print(f'HTTP error occurred: {http_err}')
                    print(response.json())
            except Exception as err:
                print(f'Other error occurred: {err}')
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
            time.sleep(self.retry_interval)
            retry_count = retry_count + 1
            if retry_count > self.max_retry_count:
                print('HTTP Error: Exceeded maximum retries')
                return {'status': 'failed', 'message': 'HTTP Error: Exceeded maximum retries'}
        return r

    def wait_for_query(self):
        # From Pooja:  Hi Todd, I have implemented pagination and also the status (Running, queued, cancelled, failed, succeeded, error)
        # for the results. The api returns 100 items by default but you can configure that using page_size parameter
        # in the url. For the pagination, I have used page_token parameter. The page_token parameter for next page
        # is given in the result json.
        # status = 'waiting'
        # retry_count = 0
        print('Waiting for results...')
        # set page size to zero for status only

#        params['page_size'] = 0
#         while True:
#             # loop until result is ready or an error has occurred
#             try:
#                 response = requests.get(self.url_result, params=self.q_params)
#
#                 # If the response was successful, no Exception will be raised
#                 response.raise_for_status()
#             except requests.HTTPError as http_err:
#                 if response.status_code != 425: # 425 is returned from lambda if query is in progress
#                     print(f'HTTP error occurred: {http_err}')
#                     print(response.json())
#             except Exception as err:
#                 print(f'Other error occurred: {err}')
#             #        else:
#
#             r = response.json()
#             # print("r ",r)
#             if 'status' in r:
#                 status = r['status']
#             if str(status).lower() == 'succeeded':
#                 break
#             if str(status).lower() == 'failed' or str(status).lower() == 'error':
#                 return {'status': 'failed', 'message': str(status).lower()}
#             print("query status: ", str(status).lower())
#             time.sleep(self.retry_interval)
#             retry_count = retry_count + 1
#             if retry_count > self.max_retry_count:
#                 print('HTTP Error: Exceeded maximum retries')
#                 return {'status': 'failed', 'message': 'HTTP Error: Exceeded maximum retries'}
        r = self.result_loop(self.url_result, self.q_params)

        if 'file_url' in r:
            self.result_url = r['file_url']
        else:
            return r
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

        print("downloading CSV file ", self.result_url, ' ...')
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
                    matchupDict[key].append(self.str_to_value(value))
                row_cnt = row_cnt + 1
        f.close()

        print("read ", row_cnt, " records")

        return {'status':'success', 'message':'Successfully retrieved CSV results', 'results':matchupDict}

    # add a single key,value pair as a filter parameter for query API
    def add_parameter(self, key, value):
        self.params[key] = value

    # convenience functions for common filtering methods
    def set_columns(self, comma_list):
        self.params['columns'] = comma_list

    def add_column(self, column):
        if 'columns' not in self.params:
            self.params['columns'] = column
        else:
            self.params['columns'] = self.params['columns'] + ',' + column

    # def set_time_range(self, start_time, end_time):
    #     self.params['start_time'] = start_time
    #     self.params['end_time'] = end_time
    def set_time_range(self, start_time, end_time):
        self.params['min_datetime'] = start_time
        self.params['max_datetime'] = end_time
    def set_lat_lon_box(self, min_latitude, max_latitude, min_longitude, max_longitude):
        self.params['min_latitude'] = min_latitude
        self.params['max_latitude'] = max_latitude
        self.params['min_longitude'] = min_longitude
        self.params['max_longitude'] = max_longitude
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

    def add_gpm_version_match(self,version):
        if 'gpm_ver_like' not in self.params:
            self.params['gpm_ver_like'] = version
        else:
            self.params['gpm_ver_like'] = self.params['gpm_ver_like'] + ',' + version

    def add_gpm_version_exclude(self,version):
        if 'gpm_ver_not_like' not in self.params:
            self.params['gpm_ver_not_like'] = version
        else:
            self.params['gpm_ver_not_like'] = self.params['gpm_ver_not_like'] + ',' + version

    def add_vn_version_match(self,version):
        if 'vn_ver_like' not in self.params:
            self.params['vn_ver_like'] = version
        else:
            self.params['vn_ver_like'] = self.params['vn_ver_like'] + ',' + version

    def add_vn_version_exclude(self,version):
        if 'vn_ver_not_like' not in self.params:
            self.params['vn_ver_not_like'] = version
        else:
            self.params['vn_ver_not_like'] = self.params['vn_ver_not_like'] + ',' + version

    #NS, MS, HS, FS
    def add_scan_match(self,scan):
        if 'scan_like' not in self.params:
            self.params['scan_like'] = scan
        else:
            self.params['scan_like'] = self.params['scan_like'] + ',' + scan

    def add_scan_exclude(self,scan):
        if 'scan_not_like' not in self.params:
            self.params['scan_not_like'] = scan
        else:
            self.params['scan_not_like'] = self.params['scan_not_like'] + ',' + scan

    def add_sensor_match(self,sensor):
        if 'sensor_like' not in self.params:
            self.params['sensor_like'] = sensor
        else:
            self.params['sensor_like'] = self.params['sensor_like'] + ',' + sensor

    def add_sensor_exclude(self,sensor):
        if 'sensor_not_like' not in self.params:
            self.params['sensor_not_like'] = sensor
        else:
            self.params['sensor_not_like'] = self.params['sensor_not_like'] + ',' + sensor

    def add_vn_filename_match(self,fn):
        if 'vn_filename_like' not in self.params:
            self.params['vn_filename_like'] = fn
        else:
            self.params['vn_filename_like'] = self.params['vn_filename_like'] + ',' + fn

    def add_vn_filename_exclude(self,fn):
        if 'vn_filename_not_like' not in self.params:
            self.params['vn_filename_not_like'] = fn
        else:
            self.params['vn_filename_not_like'] = self.params['vn_filename_not_like'] + ',' + fn

    # can also use % as a wildcard, i.e. site="K%" and list of sites i.e. site="KI%,KM%,KF%"
    def add_gr_site_match(self,site):
        if 'gr_site_like' not in self.params:
            self.params['gr_site_like'] = site
        else:
            self.params['gr_site_like'] = self.params['gr_site_like'] + ',' + site
    def add_gr_site_exclude(self,site):
        if 'gr_site_not_like' not in self.params:
            self.params['gr_site_not_like'] = site
        else:
            self.params['gr_site_not_like'] = self.params['gr_site_not_like'] + ',' + site

    # def set_zfact_measured_range(self,min,max):
    #     self.params['min_zfact_measured'] = min
    #     self.params['max_zfact_measured'] = max
    # def set_zfact_corrected_range(self,min,max):
    #     self.params['min_zfact_corrected'] = min
    #     self.params['max_zfact_corrected'] = max
    # def set_grz_range(self,min,max):
    #     self.params['min_grz'] = min
    #     self.params['max_grz'] = max
    # def set_dm_range(self,min,max):
    #     self.params['min_dm'] = min
    #     self.params['max_dm'] = max
    # def set_gr_dm_range(self,min,max):
    #     self.params['min_gr_dm'] = min
    #     self.params['max_gr_dm'] = max
    # def set_site_percent_rainy_range(self,min,max):
    #     self.params['min_site_percent_rainy'] = min
    #     self.params['max_site_percent_rainy'] = max
    # def set_site_fp_count_range(self,min,max):
    #     self.params['min_site_fp_count'] = min
    #     self.params['max_site_fp_count'] = max

    def add_variable_min(self,variable,min):
        self.min_list.append(variable)
#        self.min_list.append(str(min))
        self.min_list.append(min)
    def add_variable_max(self,variable,max):
        self.max_list.append(variable)
        self.max_list.append(max)
#        self.max_list.append(str(max))
    def add_variable_range(self,variable,min,max):
        self.add_variable_min(variable,min)
        self.add_variable_max(variable,max)
    def add_difference_threshold(self,variable1, variable2, relation, value):
        self.diff_list.append(variable1)
        self.diff_list.append(relation)
        self.diff_list.append(variable2)
#        self.diff_list.append(str(value))
        self.diff_list.append(value)
        #from Pooja:
        #difference = col1, comparator, col2, value, col2, comparator, col4, value
        #I have made a group of 4, each  with col1,comparator,col2,value which generates
        # col1 - col2 comparator(<,>,>=<,>=) value. The comparators mapped  are
        # {“gte”:“>=“, “lte”:“<=“, “lt”: “<”, “gt”: “>”, “eq”: “=”} . For example :
        # difference= topheight,lt,zero_deg_height,4,bottomheight,gte,zero_deg_height,5,hid1,eq,hid2,6 will
        # generate clauses topheight - zero_deg_height < 4 AND bottomheight - zero_deg_height >= 5 AND hid1 - hid2 = 6

    # can pick only one of the following, last one called overrides
    # above and below BB are defined as above and below 750m of mean brightband
    def set_bbprox_above(self):
        if self.bbprox > 0:
            print("BBprox already specified, ignoring...")
            return
        self.bbprox = 3
        self.add_variable_range('BBprox',self.bbprox,self.bbprox)
        #self.params['BBprox'] = 3
    def set_bbprox_below(self):
        if self.bbprox > 0:
            print("BBprox already specified, ignoring...")
            return
        self.bbprox = 1
        self.add_variable_range('BBprox',self.bbprox,self.bbprox)
        #self.params['BBprox'] = 1
    def set_bbprox_within(self):
        if self.bbprox > 0:
            print("BBprox already specified, ignoring...")
            return
        self.bbprox = 2
        self.add_variable_range('BBprox',self.bbprox,self.bbprox)
        #self.params['BBprox'] = 2

    # can pick only one of the following, last one called overrides
    def set_precip_type_convective(self):
        if self.typeprecip > 0:
            print("typePrecip already specified, ignoring...")
            return
        self.typeprecip = 2
        self.add_variable_range('typePrecip',self.typeprecip,self.typeprecip)
        #self.params['typePrecip'] = 2
    def set_precip_type_stratiform(self):
        if self.typeprecip > 0:
            print("typePrecip already specified, ignoring...")
            return
        self.typeprecip = 1
        self.add_variable_range('typePrecip',self.typeprecip,self.typeprecip)
        #self.params['typePrecip'] = 1
    def set_precip_type_other(self):
        if self.typeprecip > 0:
            print("typePrecip already specified, ignoring...")
            return
        self.typeprecip = 3
        self.add_variable_range('typePrecip',self.typeprecip,self.typeprecip)
        #self.params['typePrecip'] = 3

    # 0-100 values
    def set_beam_filling_thresh_gr(self,value):
        self.add_variable_min('GR_beam',value)
    def set_beam_filling_thresh_dpr(self,value):
        self.add_variable_min('DPR_beam',value)
    # both
    def set_beam_filling_thresh(self,value):
        self.add_variable_min('DPR_beam',value)
        self.add_variable_min('GR_beam',value)

    # 0-100 values
    def set_blockage_thresh_gr(self,value):
        self.add_variable_max('GR_blockage',value)

#         Todd,
# You can use https://3gfy5bwy4b.execute-api.us-east-1.amazonaws.com/test/?table=<name_of_the_athena_table> .
# However, even if you don’t provide the table query string parameter, it will use the table we are using for our
# latest capri core API(the staging one).
# That will give you a qid, which can be passed to the capri core fetch result api
# ( the same one that we are using for capri core result).
# https://o381wx9rqh.execute-api.us-east-1.amazonaws.com/dev/result?qid=<query id>
# Actually, the columns name are always static for a table. So, you can always use
# https://o381wx9rqh.execute-api.us-east-1.amazonaws.com/dev/result?qid=f30c3bde-e902-48a4-b887-f110ac2eb770
# as a hard link if you want to reduce the number of queries to the Athena table.

    def get_columns(self):
        try:
            #response = requests.get(url_query, params=self.params)
            response = requests.get(self.url_columns)

            # If the response was successful, no Exception will be raised
            response.raise_for_status()
        except requests.HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')  # Python 3.6
            return {'status': 'failed', 'message': 'HTTP error occurred during column query submission'}
        except Exception as err:
            print(f'Other error occurred: {err}')  # Python 3.6
            return {'status': 'failed', 'message': 'Error occurred during column query submission'}
        #        else:
        print('Successfully connected with server, submitted column query ...')

        print(response.text)
        # #self.query_id = response.text.split('queryId=')[1].split('}')[0]
        # resp_json = json.loads(response.text)
        # #self.query_id = response.text.split('queryId:')[1].split('}')[0]
        # query_id = resp_json['queryId']
        # print(query_id)
        #q_params = 'qid='+ response.text
        q_params = {'qid': response.text.strip('"')}
        print('Waiting for columns...')

        r = self.result_loop(self.url_columns_result, q_params)

        if 'file_url' in r:
            result_url = r['file_url']
        else:
            return r
        print("Athena result URL: ", result_url)

        # if 'data' in r:
        #     data = r['data']
        #     resp_json = json.loads(response.text)
        #     for
        # else:
        #     return {'status': 'failed', 'message': 'Error missing data in column query'}

        #file_name = self.result_url.split("/")[-1]
        #f = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
        #temp_filename=f.name
        #temp_filename="temp_column.json"

        print("downloading CSV column file ", result_url, ' ...')
        get_response = requests.get(result_url, stream=True)
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as f:
            temp_filename = f.name
            for chunk in get_response.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
        # print("temp_filename ", temp_filename)


        #get_response = requests.get(result_url, stream=True)
        #        with open(file_name, 'wb') as f:
        #with open(temp_filename, 'wb') as f:
        #     for chunk in get_response.iter_content(chunk_size=1024):
        #         if chunk:  # filter out keep-alive new chunks
        #             f.write(chunk)
        # print("temp_filename ", temp_filename)
        #f.close()
        try:
            lines = []
            with open(temp_filename, 'r') as f:
                for line in f:
                    lines.append(line.strip())
            #f.close()
        except IOError:
            print("Could not read column file:" + temp_filename)
            return {'status': 'failed', 'message': 'Could not read column file:' + temp_filename}

        return {'status':'success', 'message':'Successfully downloaded columns names', 'columns':lines}

class MRMSToGPM:
    def __init__(self, mrms_filename):
        self.MRMS=None
        self.GPM=None
        self.GPMFootprint=None
        self.MRMSDeepLearning=None # placeholder for eventual loading of deep learning results
        self.load_data(mrms_filename)

    def __del__(self):
        # Destructor:
        pass
    class binaryFile:
        def __init__(self,filename):
            self.data = None
            self.width = None # longitude direction
            self.height = None # latitude direction
            self.llLat = None # lower left corner
            self.llLon = None
            self.llResolution = None
            self.header_size = 5
            # note: data is written in ascending lat, lon (i.e. lower left on map)
            # if you want to access data in image style coordinate (i.e. upper left descending)
            # you need to flip the line order of the numpy array by setting flip_flag = True
            self.flip_flag = False

            self.load_data(filename)
        # convert x and y coordinates to lat, lon
        def get_lat(self,y):
            # note, fixed bug here, added "not"
            if not self.flip_flag:
                lat = y * self.llResolution + self.llLat
            else:
                lat = (self.height - y - 1) * self.llResolution + self.llLat
            return lat

        def get_lon(self,x):
            return x * self.llResolution + self.llLon

        def set_flip_flag(self,trueOrFalse):
            self.flip_flag = trueOrFalse

        def get_lat_lon(self, x, y):
            return (self.get_lat(y), self.get_lon(x))

        def __del__(self):
            # Destructor:
            pass
        # Charles code
        def load_data(self,path):
            with open(path, 'rb') as data_file:
                # note: data is written in ascending lat, lon (i.e. lower left on map)
                # if you want to access data in image style coordinate (i.e. upper left descending)
                # you need to flip the line order of the numpy array
                header = struct.unpack('>{0}f'.format(self.header_size), data_file.read(4 * self.header_size))
                self.width = int(header[0])
                self.height = int(header[1])
                self.llLat = header[2]
                self.llLon = header[3]
                self.llResolution = header[4]
                size = int(self.width * self.height)
                shape = (int(self.height), int(self.width))

                data = struct.unpack('>{0}f'.format(size), data_file.read(4 * size))
                self.data = np.asarray(data).reshape(shape)
                # flip line order if flip_flag is set to true
                #if self.flip_flag:
                #    self.data = np.flip(self.data, 0)  # data is packed in ascending latitude
        def get_data(self):
            if self.flip_flag:
                return np.flip(self.data, 0)  # data is packed in ascending latitude
            else:
                return self.data  # data is packed in ascending latitude

    def load_data(self, mrms_filename):
        # construct footprint and GPM filenames from MRMS filename
        self.MRMS = self.binaryFile(mrms_filename)
        gpm_filename = mrms_filename.split('.mrms.bin')[0]+'.gpm.bin'
        self.GPM = self.binaryFile(gpm_filename)
        fp_filename = mrms_filename.split('.mrms.bin')[0]+'.fp.bin'
        self.GPMFootprint = self.binaryFile(fp_filename)

    def set_flip_flag(self,value):
        self.MRMS.set_flip_flag(value)
        self.GPM.set_flip_flag(value)
        self.GPMFootprint.set_flip_flag(value)
