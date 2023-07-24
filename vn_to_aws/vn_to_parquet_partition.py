# ---------------------------------------------------------------------------------------------
#
#  vn_to_parquet_partition.py
#
#  Description: This program processes local subdirectories of GPM VN files, formatted in netCDF
#               and parses out the values and formats output for JSON and Parquet format
#               with directory partitions in HIVE format to AWS athena import using GLUE crawler.
#               This is a stand-alone version the VN to Athena conversion process that produces a
#               local output directory tree.
#
#  Syntax: vn_to_parquet_partition <vn_base_dir> <output_base_dir> <BB_filename> <start_YYMMDD> <end_YYMMDD>
#          start and end dates are inclusive
#
# ---------------------------------------------------------------------------------------------
import copy
# --Do all the necessary imports

import sys
import os
import json2parquet

import json

from extract_vn import process_file
print_json = False

def main():

    if len(sys.argv) < 5:
 #       print("Usage: python vn_to_parquet_partition.py <vn_base_dir> <output_base_dir> <BB_filename> <start YYMMDD> <end YYMMDD>")
        print("Usage: python vn_to_parquet_partition.py <vn_base_dir> <output_base_dir> <start YYMMDD> <end YYMMDD>")
        print("Note: start and end dates are inclusive")
        sys.exit()

    VN_DIR = sys.argv[1]
    OUT_DIR = sys.argv[2]
    # alt_bb is now freezing_level_height variable in matchup files
#    alt_bb_file = sys.argv[3]
    START_DATE = int(sys.argv[3])
    END_DATE = int(sys.argv[4])

    #alt_bb_file = 'GPM_rain_event_bb_km.txt'

    # config = {
    #     "VN_DIR": "/data/gpmgv_test/netcdf/geo_match/GPM/2ADPR/FS_Ku/V07A/2_1",
    #     "OUT_DIR": "/data/gpmgv_test/aws_athena/parquet",
    #     "JSON_DIR": "/data/gpmgv_test/aws_athena/json",
    #     "alt_bb_file": "/data/gpmgv_test/BB/GPM_rain_event_bb_km.txt",
    #     "save_json": True,
    #     "reprocess_flag": False
    # }
    #"site_pattern": "K",
    #config_file = "run_dprgmi.json"

    # support single file processing, if VN_FILE is defined, only process single file
    #config['VN_FILE'] = 'filename'
    # dirwalk=[]
    # if config.has_key('VN_FILE'):
    #     dirwalk.append((config['VN_DIR'],[],config['VN_FILE'])) # append as tuple
    # else:
    #     for root, dirs, files in os.walk(config['VN_DIR'], topdown=False):
    #         dirwalk.append((root, dirs, files)) # append as tuple
    #for root, dirs, files in dirwalk:

    # user netcdf variable names, these will be made lower case for partitions
    # fixed_partitions are the same for each record within a file.  Currently only implemented
    # fixed partitions in this code that are common within a single file (i.e. file level metadata)
    #fixed_partitions = ['GPM_ver', 'sensor', 'scan', 'year', 'month']
    #fixed_partitions = ['sensor', 'scan', 'GR_site', 'year', 'month']
    fixed_partitions = ['scan', 'GR_site', 'year']

    for root, dirs, files in os.walk(VN_DIR, topdown=False):
        for file in files:

            #print('file ' + file)
            # only process zipped nc VN files
            # if len(config['site_pattern'])>0:
            #     if file.split('.')[1].startswith(config['site_pattern']):
            #         do_file=True
            #     else:
            #         do_file = False
            # else:
            file_date = int(file.split('.')[2])
            if not ( file_date >= START_DATE and file_date <= END_DATE):
                #print('skipping file: ' + file)
                continue

            if file.endswith('.nc.gz'):
                print('processing file: ' + file)
                have_mrms, outputJson = process_file(os.path.join(root,file))

                # no precip volumes were found, skip file
                if len(outputJson) == 0:
                    print("found no precip in file " + file + " skipping...")
                    continue
                #print(outputJson)

                # special fields for partitions since these get popped out of outputJson
                site = outputJson[0]['GR_site']
                year = outputJson[0]['year']

                # use sensor as top level directory since each sensor will use a different database
                sensor = outputJson[0]['sensor']

                if 'error' in outputJson:
                    print('skipping file ', file, ' due to processing error...')
                    continue
                #print (outputJson)

                # configure output file path
                # need to remove partitioned fields from the final parquet file before writing
                # delete fields from outputJson
                # use outputJson.pop("key")
                if sensor.lower() == 'dprgmi' and 'scan' in fixed_partitions:
                    # this logic assumes scan is a partition, must split processing for each scan
                    # only DPRGMI contains multiple scan types in netCDF file

                    for scan_to_do in ['FS','NS']:
                        # set up fixed partitions for this file
                        out_path = OUT_DIR + '/' + 'parquet' + '/' + sensor.lower()
                        out_path_json = OUT_DIR + '/' + 'json'
                        out_path_metadata = OUT_DIR + '/' + 'meta'
                        # use first record to set output path for fixed partitions
                        for partition in fixed_partitions:
                            if partition == 'scan':
                                out_path = out_path + '/' + partition.lower() + '=' + scan_to_do
                                out_path_json = out_path_json + '/' + partition.lower() + '=' + scan_to_do
                            else:
                                out_path = out_path + '/' + partition.lower() + '=' + str(outputJson[0][partition])
                                out_path_json = out_path_json + '/' + partition.lower() + '=' + str(outputJson[0][partition])
                        # need to remove partitioned fields from the final parquet file before writing
                        # process all records in the file, removing partitions and adding to new output list
                        outputList = []

                        for record in outputJson:
                            # use deepcopy because pop affects original outputJson due to reference variable
                            # in python, since we are reprocessing outputJson for each scan, original must not
                            # be modified
                            temp = copy.deepcopy(record)
                            #print('record ',record)
                            if record['scan'] == scan_to_do:
                                for partition in fixed_partitions:
                                    temp.pop(partition)
                                outputList.append(temp)

                        parquet_output_file = os.path.join(out_path+'/'+file+'.parquet')

                        parquet_data = json2parquet.ingest_data(outputList)
                        os.makedirs(os.path.join(out_path), exist_ok=True)

                        # write the final parquet file
                        json2parquet.write_parquet(parquet_data, parquet_output_file, compression='snappy')
                        # save json files for testing
                        if print_json:
                            json_output_file = os.path.join(out_path_json+'/'+file+'.json')
                            os.makedirs(os.path.join(out_path_json), exist_ok=True)
                            with open(json_output_file, 'w') as json_file:
                                json.dump(outputList, json_file)
                else:
                    # set up fixed partitions for this file
                    out_path = OUT_DIR + '/' + 'parquet' + '/' + sensor.lower()
                    out_path_json = OUT_DIR + '/' + 'json'
                    out_path_metadata = OUT_DIR + '/' + 'meta'

                    # use first record to set output path for fixed partitions
                    for partition in fixed_partitions:
                        out_path=out_path+'/'+partition.lower()+'='+str(outputJson[0][partition])
                        out_path_json=out_path_json+'/'+partition.lower()+'='+str(outputJson[0][partition])

                    # need to remove partitioned fields from the final parquet file before writing
                    # process all records in the file, removing partitions and adding to new output list
                    outputList = []
                    for record in outputJson:
                        for partition in fixed_partitions:
                            record.pop(partition)
                        outputList.append(record)

                    parquet_output_file = os.path.join(out_path+'/'+file+'.parquet')

                    parquet_data = json2parquet.ingest_data(outputList)
                    os.makedirs(os.path.join(out_path), exist_ok=True)

                    # write the final parquet file
                    json2parquet.write_parquet(parquet_data, parquet_output_file, compression='snappy')
                    # save json files for testing
                    if print_json:
                        json_output_file = os.path.join(out_path_json+'/'+file+'.json')
                        os.makedirs(os.path.join(out_path_json), exist_ok=True)
                        with open(json_output_file, 'w') as json_file:
                            json.dump(outputList, json_file)

                #write metadata for file
                metadata = {"site": site,"year":year,"month":outputJson[0]["month"],
                            "day":outputJson[0]["day"],"time": outputJson[0]["time"], "site_rainy_count": outputJson[0]["site_rainy_count"],
                            "site_fp_count": outputJson[0]["site_fp_count"],
                            "site_percent_rainy": outputJson[0]["site_percent_rainy"],
                            "freezing_level_height": outputJson[0]["freezing_level_height"], "have_mrms": have_mrms}

                os.makedirs(os.path.join(out_path_metadata + '/' + site), exist_ok=True)
                with open(out_path_metadata + '/' + site + '/' + file + '.meta.json', 'w') as json_file:
                    json.dump(metadata, json_file)

                #sys.exit()

if __name__ == '__main__':
   main()
