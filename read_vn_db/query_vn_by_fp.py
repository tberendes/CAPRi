# ---------------------------------------------------------------------------------------------
#
#  query_vn_by_fp.py
#
#  Author:  Todd Berendes, UAH ITSC, March 2021
#
#  Description: this script queries data from the VN database on AWS corresponding to the
#               footprint (fp) image generated
#               by the MRMS matchup program and downloads a CSV
#               result file, then parses out the values into a dictionary.  Optional saving of
#               the CSV file is supported
#
#  Syntax: currently no input parameters
#
#
#  To Do: modify to accept input parameters
#
# ---------------------------------------------------------------------------------------------

# --Do all the necessary imports
import csv
import os
import shutil
import time
import datetime
import tempfile
import struct
import numpy as np
import json

import requests

# these are the API endpoints for the query submission and retrieval of results
# they are experimental, and subject to change
# original version
#url_query = 'https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/test3/'
#url_result = 'https://e3x3fqdwla.execute-api.us-east-1.amazonaws.com/test3/result/'

# new version with 'fp' and 'zero_deg_height'
url_query = 'https://o381wx9rqh.execute-api.us-east-1.amazonaws.com/dev/'
url_result = 'https://o381wx9rqh.execute-api.us-east-1.amazonaws.com/dev/result'

max_retry_count = 30000
retry_interval = 2

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
            self.width = None
            self.height = None
            self.llLat = None
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
            if self.flip_flag:
                lat = y * self.llResolution + self.llLat
            else:
                lat = (self.height - y - 1) * self.llResolution + self.llLat
            return lat

        def get_lon(self,x):
            return x * self.llResolution + self.llLon

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
                if self.flip_flag:
                    self.data = np.flip(self.data, 0)  # data is packed in ascending latitude

    def load_data(self, mrms_filename):
        # construct footprint and GPM filenames from MRMS filename

        self.MRMS = self.binaryFile(mrms_filename)
        gpm_filename = mrms_filename.split('.mrms.bin')[0]+'.gpm.bin'
        self.GPM = self.binaryFile(gpm_filename)
        fp_filename = mrms_filename.split('.mrms.bin')[0]+'.fp.bin'
        self.GPMFootprint = self.binaryFile(fp_filename)

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
        # delete previous query temporary CSV file is present
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
            response = requests.post(url_query, self.get_params_json())

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

    def wait_for_query(self):
        # From Pooja:  Hi Todd, I have implemented pagination and also the status (Running, queued, cancelled, failed, succeeded, error)
        # for the results. The api returns 100 items by default but you can configure that using page_size parameter
        # in the url. For the pagination, I have used page_token parameter. The page_token parameter for next page
        # is given in the result json.
        status = 'waiting'
        retry_count = 0
        print('Waiting for results...')
        # set page size to zero for status only

#        params['page_size'] = 0
        while True:
            # loop until result is ready or an error has occurred
            try:
                response = requests.get(url_result, params=self.q_params)

                # If the response was successful, no Exception will be raised
                response.raise_for_status()
            except requests.HTTPError as http_err:
                if response.status_code != 425: # 425 is returned from lambda if query is in progress
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

    # add a single key,value pair as a filter parameter
    def add_parameter(self, key, value):
        self.params[key] = value

    # convenience functions for common filtering methods
    def set_columns(self, comma_list):
        self.params['columns'] = comma_list
    # def set_time_range(self, start_time, end_time):
    #     self.params['start_time'] = start_time
    #     self.params['end_time'] = end_time
    def set_time_range(self, start_time, end_time):
        self.params['min_datetime'] = start_time
        self.params['max_datetime'] = end_time
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
    def set_vn_filename(self,type):
        self.params['vn_filename_like'] = type

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
#        self.min_list.append(str(min))
        self.min_list.append(min)
    def add_max_filter(self,variable,max):
        self.max_list.append(variable)
        self.max_list.append(max)
#        self.max_list.append(str(max))
    def add_range_filter(self,variable,min,max):
        self.add_min_filter(variable,min)
        self.add_max_filter(variable,max)
    def add_difference_threshold_filter(self,variable1, variable2, relation, value):
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
    def set_above_bb(self):
        self.add_range_filter('BBprox',3,3)
        #self.params['BBprox'] = 3
    def set_below_bb(self):
        self.add_range_filter('BBprox',1,1)
        #self.params['BBprox'] = 1
    def set_within_bb(self):
        self.add_range_filter('BBprox',2,2)
        #self.params['BBprox'] = 2

    # can pick only one of the following, last one called overrides
    def set_convective(self):
        self.add_range_filter('typePrecip',2,2)
        #self.params['typePrecip'] = 2
    def set_stratiform(self):
        self.add_range_filter('typePrecip',1,1)
        #self.params['typePrecip'] = 1
    def set_other_precip(self):
        self.add_range_filter('typePrecip',3,3)
        #self.params['typePrecip'] = 3

    # 0-100 values
    def set_beam_filling_thresh_gr(self,value):
        self.add_min_filter('GR_beam',value)
    def set_beam_filling_thresh_dpr(self,value):
        self.add_min_filter('DPR_beam',value)
    # both
    def set_beam_filling_thresh(self,value):
        self.add_min_filter('DPR_beam',value)
        self.add_min_filter('GR_beam',value)

    # 0-100 values
    def set_blockage_thresh_gr(self,value):
        self.add_max_filter('GR_blockage',value)

def main():

    # uncomment this to pass parameters
    # if len(sys.argv)>1:
    #     mrms_filename = sys.argv[1]
    # else:
    #     print("usage: query_vn_by_fp /path/to/mrms_filename.bin")
    #     sys.exit(-1)

    #testing
    mrms_filename = '/data/capri_test_data/VN/mrms_geomatch/2020/GRtoDPR.KABR.200312.34295.V06A.DPR.NS.1_21.nc.gz.mrms.bin'

    # load mrms, gpm, and footprint .bin files int MRMSToGPM class variable MRMSMatch
    # assumes filename format used in the Java VN matchup program for MRMS data
    # and also assumes that the footprint and GPM images (.bin files) are in the same directory
    MRMSMatch = MRMSToGPM(mrms_filename)

    print('fp ', MRMSMatch.GPMFootprint.data[0][0], ' gpm ', MRMSMatch.GPM.data[0][0], ' mrms ', MRMSMatch.MRMS.data[0][0])
    (lat,lon) = MRMSMatch.MRMS.get_lat_lon(0,0)
    print('lat ', lat, ' lon ', lon)

    # use fields from the filename to query database to get only matching site data
    #GRtoDPR.KABR.200312.34295.V06A.DPR.NS.1_21.nc.gz.mrms.bin
    # Parse filename to set query parameters to retrieve VN data from Athena
    parsed=mrms_filename.split('.')
    site = parsed[1]
    date_field = parsed[2]
    year='20'+date_field[0:2]
    month=date_field[2:4]
    day=date_field[4:6]
    # may want to add orbit number to database
    orbit=parsed[3]
    # for now do a "like" match on the begining of the filename up to the orbit number
    gpm_version=parsed[4]
    vn_fn = os.path.basename(mrms_filename).split('.'+gpm_version+'.')[0]+'.'
    sensor=parsed[5]
    scan=parsed[6]
    vn_version=parsed[7].replace('_','.')

    # set up db query and fp based dictionary of results

    #    params = {'start_time': "2019-03-21 00:00:00", 'end_time': "2019-04-21 00:00:00"}
    # initialize query class to start a new query
    query = VNQuery()

    # initialize query parameters
    # set columns to retrieve from database
    columns='fp,time,latitude,longitude,GR_Z,zFactorCorrected,GR_RC_rainrate,PrecipRate,typePrecip,BBheight,meanBB,BBprox,topHeight,bottomHeight'
    query.set_columns(columns)

    query.set_scan(scan)
    query.set_sensor(sensor)
    query.set_time_range(year+"-"+month+"-"+day+" 00:00:00", year+"-"+month+"-"+day+" 23:59:59")
    query.set_vn_filename(vn_fn+"%") # use filaneme to restrict orbit since orbit isn't in DB yet
    query.set_gpm_version(gpm_version)
    query.set_vn_version(vn_version)
    query.set_gr_site(site)

    print(query.params)

    ts = datetime.datetime.now().timestamp()
    print("start time: ", ts)

    # submit query to AWS
    res = query.submit_query()
    if res['status'] != 'success':
        print("Query failed: ", res['message'])
        exit(-1)

    # download csv file, may specify optional filename
    # if optional filename is ommitted, uses temporary file
    # which is automatically deleted on exit of program
    # check 'status' entry for 'success' or 'failed'
    # this function loops until success or failure
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
    if 'results' in result: # successful query with results returned
        # extract matchups dictionary from result dictionary
        matchups = result['results']
    else:
        print("Query found no matchups")
        exit(0)

    if len(matchups)==0: # successful query with empty results
        print("Query found no matchups")
        exit(0)

    # construct a dictionary indexed by fp containing lists of dictionaries of columns
    column_names=["latitude","longitude","topHeight","bottomHeight","GR_Z","zFactorCorrected","GR_RC_rainrate","PrecipRate","typePrecip"]
    index=0
    VN_data = {}
    for fp in matchups['fp']:
        # assume fp is in the returned matchups, add rest of the fields to the fp indexed dictionary
        if fp not in VN_data.keys():
            VN_data[fp] = [] # empty list
        fp_dict= {}
        for col in column_names:
            fp_dict[col] = matchups[col][index]
            pass
        VN_data[fp].append(fp_dict)
        index = index + 1
    keyList = list(VN_data.keys())

    # we now have an fp indexed list of VN volumes in VN_data
    print("keys ",keyList)
    print(VN_data[keyList[0]])

    # # Now we can loop through MRMSMatch class MRMS gridded GPMFootprint image
    # to retrieve VN query information for each matching MRMS value

    for row in range(MRMSMatch.GPMFootprint.height): # image rows
        for col in range(MRMSMatch.GPMFootprint.width): #image columns
            fp = int(MRMSMatch.GPMFootprint.data[row][col]) # need to convert to int to index in VN_data
            mrms_precip = MRMSMatch.MRMS.data[row][col]
            gpm_precip = MRMSMatch.GPM.data[row][col]
            if fp > 0 and fp in VN_data.keys(): # if fp >0 the row,col value for MRMS falls within a GPM footprint in the VN with precip
                print("fp: ", fp, " mrms precip ", mrms_precip, " gpm precip ", gpm_precip)
                # example loop to extract individual vn volumes from the footprint indexed VN_data class
                volumes = VN_data[fp]
                for volume in volumes: # loop through individual VN volume in GPM footprint (multiple elevations)
                    print("   bottomHeight", volume["bottomHeight"], " GR_RC_rainrate ", volume["GR_RC_rainrate"])
                # exit early for testing ***********************************
                exit(0)
            # else: # MRMS image value is not within a GPM footprint
            #     print("no matching VN volume for MRMS data ")

    # elapsed time for metrics
    endts = datetime.datetime.now().timestamp()
    print("end time: ", endts)

    diff = endts - ts
    print("elapsed time ", diff, "secs")

    # delete CSV file if done with it, only necessary if you supplied a filename to query.download_csv()
    # query.delete_csv()

if __name__ == '__main__':
   main()
