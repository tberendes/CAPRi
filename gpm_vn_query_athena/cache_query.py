"""
Authors: Pooja Khanal, Todd Berendes
Module to return cache qid if the event is present in the cache
This module acts as a helper to the corresponding lambda_function.py
"""
import time
import boto3
import botocore

from environment import *
s3 = boto3.resource('s3')

def get_cache(event, fields, client, table):
    # description: to get the cache for the event if the cache is present
    # parameters: event: that is passed to the lambda, client: Athena Client for data processing
    # return: existing qid or event converted to string

    # Sort the event first so that any order of the event works well
    params = event

    if 'columns' in params:
        params.pop('columns')
        #print("columns in items")
 #   event_string = str(event).lower() + str(fields).lower() + table.lower()
    #print("params ",params)
    params = sorted(params.items())
    #print("params sorted ",params)
    event_string=''
    for item in params:
        #print("item",item)
        event_string = event_string + str(item).strip().lower()
    #print('event_string - '+event_string)

    #print('event - ',event)
    #print('fields - ', fields)
    new_fields = []
    for fld in fields:
        new_fields.append(str(fld).strip().lower())
    col_str = "_".join(sorted(new_fields))
    #print('col_str - ',col_str)

    specialChars = "': ,{}%[]"
    for specialChar in specialChars:
        event_string = event_string.replace(specialChar, '_')

    #filter_cache_string = f"SELECT qid from {cache_table} WHERE query LIKE '{event_string}%'"
    filter_cache_string = f"SELECT qid from {cache_table} WHERE query LIKE '{event_string}%' AND columns LIKE '{col_str}%'"

    # query to retrieve if a cache is present
    cache_query = client.start_query_execution(
        QueryString=str(filter_cache_string),
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': f"s3://{output_bucket}"
        },
        WorkGroup=workgroup)

    cache_qid = cache_query['QueryExecutionId']
    query_status = client.get_query_execution(QueryExecutionId=cache_qid)['QueryExecution']['Status']['State']

    while query_status != 'SUCCEEDED':
        if query_status is ['FAILED', 'CANCELLED']:
            return { "status":"FAILED" }
        time.sleep(1)
        query_status = client.get_query_execution(QueryExecutionId=cache_qid)['QueryExecution']['Status']['State']

    response = client.get_query_results(
        QueryExecutionId=cache_qid
    )

    # if cache entry in db is found, return qid, else return the event string
    if len(response['ResultSet']['Rows']) > 1:
        qid = response['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
        # look in s3 query output location and check for presence of cache file
        try:
            s3.Object(output_bucket, qid + '.csv').load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                print("Cache entry file missing or expired")
                return {"status": "MISSING", "event_string": event_string}

        print("Cache entry file exists")
        return {"status": "FOUND", "qid": qid}

        # exists = s3.list_objects_v2(output_bucket, qid + '.csv')
        # if 'Contents' in exists:
        #     print("Cache entry file exists")
        #     return {"status": "FOUND", "qid": qid}
        # else: # file is missing or has been deleted by age
        #     print("Cache entry file missing or expired")
        #     return {"status": "MISSING", "event_string":event_string}
        # #return qid
    else:
        return {"status": "MISSING", "event_string":event_string}
        #return event_string
