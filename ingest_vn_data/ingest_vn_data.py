import base64
import string
import json
import random
import boto3 as boto3

data_bucket = "capri-data"

s3 = boto3.resource(
    's3')

#API key: LakZ1uMrR465m1GQKoQhQ7Ig3bwr7wyPavUZ9mEc
#https://6inm6whnni.execute-api.us-east-1.amazonaws.com/default/ingest_vn_data

def lambda_handler(event, context):

    print("event ", event)

    if 'body' in event:
        try:
            # AWS API gateway automatically encodes the body if an API key is specified on the original
            # API gateway post
            if event['isBase64Encoded'] == True:
                message_bytes = base64.b64decode(event['body'])
                message = message_bytes.decode('ascii')
                event = json.loads(message)
                print("decoded event ", event)
            else:
                event = json.loads(event['body'])
        except (TypeError, ValueError):
            return dict(statusCode='200',
                        headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                                 'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                                 'Access-Control-Allow-Methods': 'DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT'},
                        body=json.dumps({'message': "missing json parameters"}), isBase64Encoded='false')

    #    "dataset": "precipitation", "org_unit": "district", "agg_period": "daily", "start_date": "1998-08-21T17:38:27Z",
#    "end_date": "1998-09-21T17:38:27Z", "data_element_id": "fsdfrw345dsd"
#    dataset = event['dataset']
#    org_unit = event['org_unit']
#    period = event['agg_period']
#    start_date = event['start_date']
#    end_date = event['end_date']
#    data_element_id = event['data_element_id']
#    boundaries = event['boundaries']
#    request_id = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(10))

    # added for new json format
#    districts = boundaries

    # format new json structure
#    downloadJson = {"dataset": dataset, "org_unit": org_unit, "agg_period": period, "start_date": start_date,
#        "end_date": end_date, "data_element_id": data_element_id, "request_id": request_id,
#        "min_lat": minlat, "max_lat": maxlat, "min_lon": minlon, "max_lon": maxlon}

#    download_param_pathname = ""
#    if dataset.lower() == 'precipitation':
#        download_param_pathname="requests/download/"+dataset+ "/"
        #set up download_imerg data
#    else:
 #       return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
#                                               'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
#                                               'Access-Control-Allow-Methods': 'DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT'},
#                    body=json.dumps({'message': "illegal dataset: " + dataset}), isBase64Encoded='false')

#    outJson = {"request_id": request_id, "boundaries": districts}


    with open("/tmp/test.json", 'w') as json_file:
        json.dump(event, json_file)
    #        json.dump(districtPrecipStats, json_file)
    json_file.close()

    s3.Bucket(data_bucket).upload_file("/tmp/test.json",
                                       "test/test_upload.json")

     # set a random jobID string for use in all subsequent processes
#    return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
#                body=json.dumps({'files': download_results}), isBase64Encoded='false')
    return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                                           'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                                           'Access-Control-Allow-Methods': 'DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT'},
                body=json.dumps({'status': "upload successful"}), isBase64Encoded='false')


#return dict(statusCode='200', body={'files': download_results}, isBase64Encoded='false')
#    return dict(body={'files': download_results}, isBase64Encoded='false')

    # return {
    #     'files': download_results
    # }
