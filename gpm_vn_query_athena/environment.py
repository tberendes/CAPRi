# read all the environment variables

import os

output_bucket = os.environ.get('OUTPUT_BUCKET')
cache_table = os.environ.get('CACHE_TABLE')
database =  os.environ.get('DATABASE')
workgroup = os.environ.get('WORKGROUP')

