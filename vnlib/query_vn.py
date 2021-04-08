# ---------------------------------------------------------------------------------------------
#
#  query_vn.py
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
import vnlib

def main():

    ts = datetime.datetime.now().timestamp()
    print("start time: ", ts)

#    params = {'start_time': "2019-03-21 00:00:00", 'end_time': "2019-04-21 00:00:00"}
    # initialize query class to start a new query
    query = vnlib.VNQuery()

    # initialize query parameters
    query.set_time_range("2019-03-21 00:00:00", "2019-03-24 00:00:00")
    query.set_columns("time,latitude,longitude,GR_Z,Dm,gr_site,vn_filename,raynum,scannum,elev,typePrecip,BBheight,meanBB,BBprox,GR_beam,DPR_beam,GR_blockage")
    #query.set_columns("bottomHeight,zero_deg_height,BBheight,meanBB,BBprox,GR_beam,DPR_beam,GR_blockage")
    # added new column for s2ku adjusted GR reflectivity
    # column:  'GR_Z_s2ku'

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

    # convenience functions for setting up filtering options commonly used in VN analysis
    # query.set_beam_filling_thresh_gr(value_0_100)
    # query.set_beam_filling_thresh_dpr(value_0_100)
    # query.set_beam_filling_thresh(value_0_100)
    # query.set_blockage_thresh(value_0_100)

    # These filters can be used in combination to replicate the filtering of our old IDL plots
    # I tried to duplicate the algorithms Bob used in the IDL code and created a meanBB variable for each
    # netCDF file which is what Bob used for BB calculations.  The BBprox and DPR_beam and
    # GR_beam variables are created using my best interpretation of his IDL code.  I have also tried to
    # implement the GR blockage parameter (if present) and the alternative BB based on Ruc 0 height soundings
    # as a fallback if DPR-based BB is missing.

    query.set_beam_filling_thresh(100.0) # this was the default parameter in our old plots
    #query.set_below_bb() # above and below BB are defined as above and below 750m of mean brightband
    #query.set_above_bb()
    #query.set_convective()
    #query.set_stratiform()


    #Here’s what I have in mind for this…specify the parameter ‘topheight_below_ruc_0’=[some_value]
    # and it queries the zero_deg_height column and topHeight column and returns the results
    # where topHeight-zero_deg_height < [some_value]. Similarly for the bottomHeight…’bottomheight_above_ruc_0’=[some_value]
    # which queries the zero_deg_height and bottomHeight columns and returns results where bottomHeight-zero_deg_height > [some_value].

    # can use 'lt', 'lte', 'gt', 'gte', 'eq' for relation
    #query.add_difference_threshold_filter('topHeight', 'zero_deg_height', 'lt', 1)
    query.add_difference_threshold('bottomHeight', 'zero_deg_height', 'lt', -1)

    #query.add_range_filter('zero_deg_height', 0.0, 2.0)

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

# exit early
    #exit(0)

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
    #for key,values in matchups.items():
    #    print("key ", key, " value[0] ", values[0])
    #for key,values in matchups.items():
    #    print("key ", key, " value[-1] ", values[-1])

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
