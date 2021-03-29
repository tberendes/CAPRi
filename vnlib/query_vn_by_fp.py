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
import os
import datetime
import vnlib

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
    MRMSMatch = vnlib.MRMSToGPM(mrms_filename)

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
    query = vnlib.VNQuery()

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
