"""
Authors : Pooja Khanal, Todd Berendes
This is the first part of the realtime querying for capri. It takes some
query parameters as input, saves the query result into a csv file in
corresponding S3 bucket and gives the execution id as output.
Also, the cache implementation for already existing query is also performed in
this lambda.

"""
import os
import boto3
from get_query import get_query
from cache_query import get_cache
import json

from get_schema import *

client = boto3.client('athena')

def save_cache(cache_string, query_id, columns):
    # description: to save the cache for a new query
    # parameters: cache_string used to save the cache and the corresponding qid
    # return: None

    save_cache_string = f"INSERT INTO {cache_table} VALUES {cache_string , query_id, columns}"
    # Save query and key and query_id as value
    # Cache query to save the data
    cache_query = client.start_query_execution(
        QueryString = str(save_cache_string),
        QueryExecutionContext = {
            'Database' : database
        },
        ResultConfiguration = {
            'OutputLocation' :  f"s3://{output_bucket}"
        },
        WorkGroup=workgroup)


def lambda_handler_utility(event, context, client):
    # description:- helper for the lambda function, calls another function to check for cache, runs the query and returns Query Id
    # parameters:- event and context as in lambda handler event and context, and boto3 client
    # return:- list of query ids for the corresponding query


    query_id_list = []
    cache_string_list = []
    columns_array = []
    query_list = []

    # need to set table for data type
    # make table_name a required parameter
    table = event['table_name']

    #get schema for table to determine data types of columns
    schema=get_schema(client,database,table)
    #print("schema ",schema)
    if not schema['success']:
        print("Error:  could not read schema")
        return []
    # all_parameters=[]
    # names = schema['types'].keys()
    # for name in names:
    #     all_parameters.append(name.lower())

    if 'columns' in event:
        columns = event['columns'].lower()
        columns = columns.split(',')
        fields_list = sorted(columns)
    else:
        return []
        #fields_list = sorted(all_parameters)


    # The logic for breaking queries into multiples based on number of columns
    # is not curretntly implemented due to lack of a uuid column to sort by.
    # Unless sort order of returned rows is ensured, we cannot reliably
    # guarantee the row order from one query to the next.


    # for i in range(0, len(fields_list), 13):
    #     fields = fields_list[i:min(i+13, 74)]
    #     if "cache" in event and event['cache'].lower() == "true":
    #         cache_qid = get_cache(event, fields , client)
    #         if len(cache_qid) == 36: #qid is always 36 characters in len
    #             query_id = cache_qid
    #             query_id_list.append(query_id)
    #         else:
    #             cache_string = cache_qid
    #             cache_string_list.append(cache_string)
    #             columns_array.append(fields)
    #     else:
    #         columns_array.append(fields)

# check this logic %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    fields = fields_list
    print("event looking for cache... ",event)
    if "cache" in event and event['cache']:
        cache = get_cache(event, fields , client, table)
        if "qid" in cache:
            query_id = cache["qid"]
            query_id_list.append(query_id)
        else:
            cache_string = cache["event_string"]
            cache_string_list.append(cache_string)
            columns_array.append(fields)
    else:
        columns_array.append(fields)

    if not cache_string_list and query_id_list:
        return query_id_list

    for field in fields_list:
        col_str = "_".join(field)

    new_fields = []
    for fld in fields_list:
        new_fields.append(str(fld).strip().lower())
    col_str = "_".join(sorted(new_fields))
    #print('col_str - ',col_str)

    query_list = get_query(event, columns_array)
    print("query: ", query_list)
    for i in range(len(query_list)):
        try:
            query_start = client.start_query_execution(
                QueryString = str(query_list[i]),
                QueryExecutionContext = {
                    'Database': database
                    },
                ResultConfiguration = {
                    'OutputLocation' : f"s3://{output_bucket}"
                    },
                WorkGroup=workgroup)
        # handling error exceptions
        except Exception as e:
            print(e.response)
            # map to 400 status code as Bad request
            raise Exception( json.dumps({
                "success": False,
                "message" : "Error Executing query. Check your parameters."
            }))

        query_id = query_start['QueryExecutionId']
        if "cache" in event and event['cache']:
            save_cache(cache_string_list[i], query_id, col_str)
        query_id_list.append(query_id)
    return query_id_list

def lambda_handler(event, context):
    # description: To Exceute the Athena Query with the event
    # parameters: User passed event and the lambda context
    # return: query id which goes to another lambda (capri_return_query_result) through API
    #print("Event ")
    #print(event)

    params={}
    # check for different http methods and pull out parameters accordingly
    if "httpMethod" in event and event["httpMethod"] == "GET":
        if "queryStringParameters" in event and event["queryStringParameters"] is not None:
            params = event["queryStringParameters"]
            #print(params)
        print("GET...")
    elif "httpMethod" in event and event["httpMethod"] == "POST": # post
        if "body" in event and event["body"] is not None:
            params = json.loads(event["body"])
        print("POST...")
    else: # direct access from AWS API, not using https i.e. test trigger
        print("DIRECT...")
        if "queryStringParameters" in event:
            params = event["queryStringParameters"]
        elif "body" in event:
            params = json.loads(event["body"])
        else:
            params = event

    if len(params) == 0:
        return {
            "success": False,
            "Message": "Provide Query Parameters"
        }
    response={}
    response["body"] = {}
    queryResponse = {}
    responseString = ''
    responseSuccess = True
    queryId = '0'
    
    #print('params:')
    #print(params)
    # table_name specifies table in database, each data type will be in a
    # separate table dpr, dprgmi, gmi (gmi not currently implemented)
    # default table use dpr

    if 'table_name' not in params:
        params['table_name'] = 'dpr'
        print('default table: dpr')

    if 'cache' not in params:
        params['cache'] = True
        print('default cache = True')

    if 'qid' in params: # only check status of query for id
        queryId = params['qid']
        print('checking status of qid ', queryId)
        # need to check exception,
        try:
            query_status = client.get_query_execution(QueryExecutionId=queryId)['QueryExecution']['Status']
            statusResponse = query_status['State']
            print('qid ', queryId, ' status ',statusResponse)
            file_url = "https://" + output_bucket + ".s3.amazonaws.com/" + queryId + '.csv'
            response['body'] = {"result_url":file_url}
        except:
            statusResponse = 'MISSING'
    #elif 'get_columns' in params and params['get_columns']: # only return columns and types in the database
    elif 'get_columns' in params: # only return columns and types in the database
           # get schema for table to determine data types of columns
        print("get_columns ",params['get_columns'])
        schema = get_schema(client, database, params['table_name'])
        print("schema ", schema)
        if not schema['success']:
            print("Error:  could not read schema")
            response['body'] = {}
            statusResponse = 'FAILED'
        else:
            names = list(schema['types'].keys())
            types = list(schema['types'].values())
            response['body'] = {"names":names,"types":types}
            statusResponse = 'SUCCEEDED'
    elif 'columns' not in params:
        print("missing columns")
        response['body'] = {"message": "Parameter error: missing columns"}
        statusResponse = 'FAILED'
    else:
        print("performing query...")
        print("params ",params)
        query_id_list = lambda_handler_utility(params, context, client)

        if len(query_id_list) == 0:
            response['body'] = {"message":"Parameter error"}
            statusResponse = 'FAILED'
        else:
            queryId = query_id_list[0]
            try:
                query_status = client.get_query_execution(QueryExecutionId=queryId)['QueryExecution']['Status']
                statusResponse = query_status['State']
            except:
                statusResponse = 'MISSING'
            # is this .txt or .csv?  check bucket
            file_url = "https://" + output_bucket + ".s3.amazonaws.com/" + queryId + '.csv'
            response['body'] = {"result_url":file_url}


        #return json.dumps({
        #    "success": True,
        #    "queryIds": query_id_list
        #})

    #this response format is required to support lambda proxy integration
    # and allow function from web browser using GET interface
    response["statusCode"] = 200
    response["headers"] = {}
    response["isBase64Encoded"] = False
    response["body"]["status"] = statusResponse
    response["body"]["qid"] = queryId

    #response["success"] = True
    #if len(query_id_list) > 0:
    #    response["queryIds"] = query_id_list


    return json.dumps(response)


