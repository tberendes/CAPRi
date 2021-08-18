'''
Author: Pooja Khanal 

This Lambda is used to create more than 100 partitions for corresponding athena
table. check for the database name, partitioned_table name, unpartitioned_table name as well as the output_bucket name

Multiple Insert into statements are needed so as each insertion statement doesn't exceed the 100 partitions limit
Insertion is done in a bunch of 6 insert into statements. 

This is done to not overload the Athena with lots of queries at a single instance.
If done so, Athena will not be able to process some queries. 

'''

import json
import boto3
import os 
import time

def create_partitions(year,month_range, qid_list, client, database, partitioned_table,unpartitioned_table, output_bucket):
    j = year
    i = month_range
    print(month_range)
    # month 1-6 
    for month in month_range:
#        partition_string = f" INSERT INTO {database}.{partitioned_table} ( \"bbheight\" , \"bbprox\" , \"bbstatus\", \"dpr_beam\",  \"dm\", \"gpm_ver\", \"gr_dm\", \"gr_dm_max\",  \"gr_dm_stddev\", \"gr_nw\", \"gr_nw_max\" , \"gr_nw_stddev\",\"gr_rc_rainrate\", \"gr_rc_rainrate_max\" , \"gr_rc_rainrate_stddev\", \"gr_rhohv\", \"gr_rr_rainrate\", \"gr_rr_rainrate_max\", \"gr_rr_rainrate_stddev\", \"gr_z\", \"gr_z_max\", \"gr_z_stddev\",\"gr_z_s2ku\", \"gr_zdr\", \"gr_beam\", \"gr_blockage\", \"gr_site\", \"nw\", \"preciprate\", \"precipratesurface\", \"surfpreciptotrate\", \"typeprecip\", \"vn_ver\", \"zfactorcorrected\", \"zfactormeasured\", \"bottomheight\", \"clutterstatus\", \"elev\", \"fp\", \"heightstormtop\", \"hid_1\", \"hid_10\", \"hid_11\", \"hid_12\", \"hid_13\", \"hid_14\", \"hid_15\", \"hid_2\", \"hid_3\", \"hid_4\", \"hid_5\", \"hid_6\", \"hid_7\", \"hid_8\", \"hid_9\", \"latitude\",\"longitude\", \"meanbb\", \"piafinal\", \"preciptotwatercont\", \"raynum\" , \"scannum\", \"site_elev\", \"site_fp_count\", \"site_percent_rainy\",\"site_rainy_count\", \"time\",\"topheight\", \"vn_filename\" ,\"zero_deg_height\",\"sensor\", \"scan\", \"year\",\"month\",\"day\") SELECT  bbheight, bbprox, bbstatus , dpr_beam , dm, gpm_ver, gr_dm , gr_dm_max, gr_dm_stddev, gr_nw, gr_nw_max, gr_nw_stddev, gr_rc_rainrate, gr_rc_rainrate_max, gr_rc_rainrate_stddev, gr_rhohv, gr_rr_rainrate, gr_rr_rainrate_max, gr_rr_rainrate_stddev, gr_z, gr_z_max, gr_z_stddev, gr_z_s2ku, gr_zdr, gr_beam, gr_blockage, gr_site, nw, preciprate, precipratesurface, surfpreciptotrate , typeprecip , vn_ver,zfactorcorrected , zfactormeasured , bottomheight , clutterstatus , elev , fp, heightstormtop, hid_1 , hid_10 , hid_11 , hid_12 , hid_13 , hid_14 , hid_15 , hid_2 , hid_3 , hid_4 , hid_5 , hid_6 , hid_7 , hid_8 , hid_9 , latitude, longitude , meanbb, piafinal, preciptotwatercont, raynum , scannum , site_elev, site_fp_count ,site_percent_rainy , site_rainy_count , time, topheight,vn_filename, zero_deg_height,sensor , scan , year ,month, day FROM {database}.{unpartitioned_table} WHERE year = {j} AND month = {month} "
        partition_string = f" INSERT INTO {database}.{partitioned_table} ( \"bbheight\" , \"bbprox\" , \"bbstatus\", \"dpr_beam\",  \"dm\", \"gpm_ver\", \"gr_dm\", \"gr_dm_max\",  \"gr_dm_stddev\", \"gr_nw\", \"gr_nw_max\" , \"gr_nw_stddev\",\"gr_rc_rainrate\", \"gr_rc_rainrate_max\" , \"gr_rc_rainrate_stddev\", \"gr_rhohv\", \"gr_rr_rainrate\", \"gr_rr_rainrate_max\", \"gr_rr_rainrate_stddev\", \"gr_z\", \"gr_z_max\", \"gr_z_stddev\",\"gr_z_s2ku\", \"gr_zdr\", \"gr_beam\", \"gr_blockage\", \"gr_site\", \"mrmsprecip\", \"nw\", \"preciprate\", \"precipratesurface\", \"surfpreciptotrate\", \"typeprecip\", \"vn_ver\", \"zfactorcorrected\", \"zfactormeasured\", \"bottomheight\", \"clutterstatus\", \"elev\", \"fp\", \"heightstormtop\", \"hid_1\", \"hid_10\", \"hid_11\", \"hid_12\", \"hid_13\", \"hid_14\", \"hid_15\", \"hid_2\", \"hid_3\", \"hid_4\", \"hid_5\", \"hid_6\", \"hid_7\", \"hid_8\", \"hid_9\", \"latitude\",\"longitude\", \"meanbb\", \"piafinal\", \"preciptotwatercont\", \"raynum\" , \"scannum\", \"site_elev\", \"site_fp_count\", \"site_percent_rainy\",\"site_rainy_count\", \"time\",\"topheight\", \"vn_filename\" ,\"zero_deg_height\",\"sensor\", \"scan\", \"year\",\"month\",\"day\") SELECT  bbheight, bbprox, bbstatus , dpr_beam , dm, gpm_ver, gr_dm , gr_dm_max, gr_dm_stddev, gr_nw, gr_nw_max, gr_nw_stddev, gr_rc_rainrate, gr_rc_rainrate_max, gr_rc_rainrate_stddev, gr_rhohv, gr_rr_rainrate, gr_rr_rainrate_max, gr_rr_rainrate_stddev, gr_z, gr_z_max, gr_z_stddev, gr_z_s2ku, gr_zdr, gr_beam, gr_blockage, gr_site, mrmsprecip, nw, preciprate, precipratesurface, surfpreciptotrate , typeprecip , vn_ver,zfactorcorrected , zfactormeasured , bottomheight , clutterstatus , elev , fp, heightstormtop, hid_1 , hid_10 , hid_11 , hid_12 , hid_13 , hid_14 , hid_15 , hid_2 , hid_3 , hid_4 , hid_5 , hid_6 , hid_7 , hid_8 , hid_9 , latitude, longitude , meanbb, piafinal, preciptotwatercont, raynum , scannum , site_elev, site_fp_count ,site_percent_rainy , site_rainy_count , time, topheight,vn_filename, zero_deg_height,sensor , scan , year ,month, day FROM {database}.{unpartitioned_table} WHERE year = {j} AND month = {month} "
        print(f"Query being executed for year = {year} and month = {month}\n")
        print(partition_string)
        print("********************************************************************")

# test query
#INSERT INTO capri_real_time_query.vn_partition ( "bbheight" , "bbprox" , "bbstatus", "dpr_beam",  "dm", "gpm_ver", "gr_dm", "gr_dm_max",  "gr_dm_stddev", "gr_nw", "gr_nw_max" , "gr_nw_stddev","gr_rc_rainrate", "gr_rc_rainrate_max" , "gr_rc_rainrate_stddev", "gr_rhohv", "gr_rr_rainrate", "gr_rr_rainrate_max", "gr_rr_rainrate_stddev", "gr_z", "gr_z_max", "gr_z_stddev","gr_z_s2ku", "gr_zdr", "gr_beam", "gr_blockage", "gr_site", "mrmsprecip", "nw", "preciprate", "precipratesurface", "surfpreciptotrate", "typeprecip", "vn_ver", "zfactorcorrected", "zfactormeasured", "bottomheight", "clutterstatus", "elev", "fp", "heightstormtop", "hid_1", "hid_10", "hid_11", "hid_12", "hid_13", "hid_14", "hid_15", "hid_2", "hid_3", "hid_4", "hid_5", "hid_6", "hid_7", "hid_8", "hid_9", "latitude","longitude", "meanbb", "piafinal", "preciptotwatercont", "raynum" , "scannum", "site_elev", "site_fp_count", "site_percent_rainy","site_rainy_count", "time","topheight", "vn_filename" ,"zero_deg_height","sensor", "scan", "year","month","day") SELECT  bbheight, bbprox, bbstatus , dpr_beam , dm, gpm_ver, gr_dm , gr_dm_max, gr_dm_stddev, gr_nw, gr_nw_max, gr_nw_stddev, gr_rc_rainrate, gr_rc_rainrate_max, gr_rc_rainrate_stddev, gr_rhohv, gr_rr_rainrate, gr_rr_rainrate_max, gr_rr_rainrate_stddev, gr_z, gr_z_max, gr_z_stddev, gr_z_s2ku, gr_zdr, gr_beam, gr_blockage, gr_site, mrmsprecip, nw, preciprate, precipratesurface, surfpreciptotrate , typeprecip , vn_ver,zfactorcorrected , zfactormeasured , bottomheight , clutterstatus , elev , fp, heightstormtop, hid_1 , hid_10 , hid_11 , hid_12 , hid_13 , hid_14 , hid_15 , hid_2 , hid_3 , hid_4 , hid_5 , hid_6 , hid_7 , hid_8 , hid_9 , latitude, longitude , meanbb, piafinal, preciptotwatercont, raynum , scannum , site_elev, site_fp_count ,site_percent_rainy , site_rainy_count , time, topheight,vn_filename, zero_deg_height,sensor , scan , year ,month, day FROM capri_real_time_query.parquet WHERE year = 2014 AND month = 4 AND day >= 0 AND day <= 2
        
        # Query String to save the data 
        query = client.start_query_execution(
            QueryString = str(partition_string),
            
            ResultConfiguration = { 
                'OutputLocation' :  f"s3://{output_bucket}"
            }) 
        qid = query['QueryExecutionId']
        qid_list.append(qid)
    return qid_list


def main():
    session = boto3.Session(profile_name='CAPRI')
    # Any clients created from this session will use credentials
    # from the [CAPRI] section of ~/.aws/credentials.
    client = session.client('athena')
#    client = boto3.client('athena')
    # first create a table manually or make the terraform to create it 
    # description: to save the cache for a new query
    # parameters: event_string used to save the cache and the corresponding qid
    # return: None

    # declare the variables 
    # Note : check for the database name, partitioned_table name, unpartitioned_table name as well as the output_bucket name

    database = "capri_real_time_query"
#    partitioned_table = "parquet_june_17_9"
#    unpartitioned_table = "parquet_02_21"
    partitioned_table = "vn_partition"
    unpartitioned_table = "parquet"
    output_bucket = "aws-athena-query-results-capri-real-time"

    #years = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]
    years = [2014]
#    months = [list(range(1,7)), list(range(7,13))]
    #months = [list(range(1,3)),list(range(3,5)),list(range(5,7)),list(range(7,9)),list(range(9,11)),list(range(11,13))]
    months = [list(range(3,5))]

#    months = [list(range(5,9)), list(range(9,13))]
#    months = [list(range(1,5)), list(range(5,9)), list(range(9,13))]
    for year in years:
        for month_range in months:
            qid_list = []
            qids = create_partitions(year, month_range ,qid_list, client, database, partitioned_table,unpartitioned_table, output_bucket)

            print("The queries with following qids are running\n")
            print(qids)
            for qid in qids:
                print("checking for qid=", qid)
                query_state = ''
                while query_state != "SUCCEEDED":
                    query_state = client.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']
                    # query_state = query_status['State']
                    if query_state in ["FAILED", "CANCELLED"]:
                        print(f"Run the query with {qid} manually from Athena interface")
                        break
                print(f"The query succeeded is qid = {qid} with status {query_state}")    
    return qids

main()
    



