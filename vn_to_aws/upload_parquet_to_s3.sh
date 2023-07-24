#!/bin/bash
# mirror the VN parquet directories on S3.
# This assumes that AWS credentials for ITSC CAPRi services have been set up for the user.
# format: aws s3 sync <directory containing dpr and dprgmi parquet directories> <s3 bucket, currently gpm-vn-data>
aws s3 sync /data/gpmgv/athena/parquet s3://gpm-vn-data
