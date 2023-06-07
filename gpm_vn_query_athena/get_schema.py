import boto3
from environment import *

import time

# note that the crawler will automatically assign partitions in the directory
# structure as string types, (i.e. year).  You need to edit the schema to change
# the partition (in this case 'year') types for integer values manually in order
# for the numeric partition variable to show up in the max_min_paramaeters and
# like_not_like_parameters lists

min_max_parameters = {}
like_not_like_parameters = {}
boolean_parameters = []
all_parameters = []

max_tries = 60

def get_schema(client, db, table):
    query = "SHOW CREATE TABLE " + table
    try:
        query_start = client.start_query_execution(
            QueryString=str(query),
            QueryExecutionContext={
                'Database': db
            },
            ResultConfiguration={
                'OutputLocation': f"s3://{output_bucket}"

            },
            WorkGroup=workgroup)

    except Exception as e:
        return {'success': False, 'message': 'get_schema could not perform query:'}

    query_id = query_start['QueryExecutionId']

    # loop until query is done

    # get the status of the query with the qid from Athena
    print(f'schema Query ID: {query_id}')

    cnt = 0
    while cnt < max_tries:
        query_status = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']
        query_state = query_status['State']
        print('schema query state: ', query_state)
        if query_state in ['QUEUED', 'RUNNING']:
            print("running...")
            cnt = cnt + 1
            time.sleep(1)
        elif query_state in ['FAILED', 'CANCELLED']:
            print("failed schema query")
            break
        else:
            break

    # time.sleep(1)
    response = {}
    # locate the csv file in S3 output bucket
    file_url = "https://" + output_bucket + ".s3.amazonaws.com/" + query_id + '.txt'

    # download file
    # print("downloading CSV column file ", result_url, ' ...')
    # get_response = requests.get(result_url, stream=True)
    # with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as f:
    #    temp_filename = f.name
    #    for chunk in get_response.iter_content(chunk_size=1024):
    #        if chunk:  # filter out keep-alive new chunks
    #            f.write(chunk)

    s3_client = boto3.client('s3')

    # Download the file from S3
    s3_client.download_file(output_bucket, query_id + '.txt', '/tmp/' + query_id + '.txt')

    try:
        types = {}
        with open('/tmp/' + query_id + '.txt', 'r') as f:
            cnt = 0
            for line in f:
                # print("line ", line)
                # skip first line and "PARTITIONED BY'
                if cnt == 0 or line.find('PARTITIONED BY') >= 0:
                    cnt = cnt + 1
                    continue
                if line.find('ROW FORMAT') >= 0:
                    break
                parsed_line = line.strip().split(' ')
                # print("var:",parsed_line[0].strip('`')," type:",parsed_line[1])
                types[parsed_line[0].strip('`')] = parsed_line[1]
                cnt = cnt + 1
    except IOError:
        print("Could not read column file:" + '/tmp/' + query_id + '.txt')
        return {'success': False, 'message': 'Could not read column file:' + '/tmp/' + query_id + '.txt'}

    # set up like_not_like and min_max parameter lists
    variable_names = types
    for name, type in variable_names.items():
        all_parameters.append(name)
        if type == 'string':  # use like/not like comparisons
            opts = [name + '_like', name + '_not_like']
            like_not_like_parameters[name] = opts
        elif type == 'boolean':  # use True/False comparisons
            boolean_parameters.append(name)
        else:  # assumed numeric, use max/min comparisons
            opts = ['min_' + name, 'max_' + name]
            min_max_parameters[name] = opts

    return {'success': True, 'message': 'Successfully downloaded types names', 'types': types}
