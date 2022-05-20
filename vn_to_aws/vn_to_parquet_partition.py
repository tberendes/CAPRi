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


# --Do all the necessary imports

import sys
import os
import json2parquet

import json

from extract_vn import read_alt_bb_file, process_file

def main():

    if len(sys.argv) < 6:
        print("Usage: python vn_to_parquet_partition.py <vn_base_dir> <output_base_dir> <BB_filename> <start YYMMDD> <end YYMMDD>")
        print("Note: start and end dates are inclusive")
        sys.exit()

    VN_DIR = sys.argv[1]
    OUT_DIR = sys.argv[2]
    alt_bb_file = sys.argv[3]
    START_DATE = int(sys.argv[4])
    END_DATE = int(sys.argv[5])

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

    # assume BB file is under VN_DIR/BB using standard name
    #bright_band = read_alt_bb_file(VN_DIR+'/BB'+alt_bb_file)
    bright_band = read_alt_bb_file(alt_bb_file)

    # TODO: need to handle missing alt_bb file, skip if not specified

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
    # fixed partitions in this code
    fixed_partitions = ['GPM_ver', 'sensor', 'scan', 'year', 'month', 'day']

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
                have_mrms, outputJson = process_file(os.path.join(root,file), bright_band)

                # no precip volumes were found, skip file
                if len(outputJson) == 0:
                    print("found no precip in file " + file + " skipping...")
                    continue
                #print(outputJson)
                if 'error' in outputJson:
                    print('skipping file ', file, ' due to processing error...')
                    continue
                print (outputJson)

                # configure output file path
                # need to remove partitioned fields from the final parquet file before writing
                # delete fields from outputJson
                # use outputJson.pop("key")

                # set up fixed partitions for this file
                out_path = OUT_DIR
                # use first record to set output path for fixed partitions
                for partition in fixed_partitions:
                    out_path=out_path+'/'+partition.lower()+'='+str(outputJson[0][partition])

                # process all records in the file, removing partitions and adding to new output list
                outputList = []
                for record in outputJson:
                    for partition in fixed_partitions:
                        record.pop(partition)
                    outputList.append(record)

                parquet_output_file = os.path.join(out_path+'/'+file+'.parquet')

                parquet_data = json2parquet.ingest_data(outputList)
                os.makedirs(os.path.join(out_path), exist_ok=True)

                # need to remove partitioned fields from the final parquet file before writing
                json2parquet.write_parquet(parquet_data, parquet_output_file, compression='snappy')

                #sys.exit()

if __name__ == '__main__':
   main()
