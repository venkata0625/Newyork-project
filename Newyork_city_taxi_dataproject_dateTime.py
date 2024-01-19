import boto3
import os
import io
import bz2
import json
from datetime import datetime

def print_min_max_pickup_datetimes_from_s3(region, bucket_name, folder_name):
    # Create an S3 client
    s3_client = boto3.client('s3', region_name=region)

    # Initialize min and max datetime variables
    min_datetime = datetime.max
    max_datetime = datetime.min

    # List objects within the specified folder
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    if 'Contents' in objects:
        for obj in objects['Contents']:
            key = obj['Key']
            if key.endswith('.bz2'):
                try:
                    # Get the object from S3
                    s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
                    s3_data = s3_object['Body'].read()

                    # Decompress the data using bz2
                    decompressed_data = bz2.decompress(s3_data)

                    # Process each line of the decompressed data
                    for line in decompressed_data.splitlines():
                        try:
                            trip = json.loads(line)
                            pickup_datetime = datetime.fromisoformat(trip['pickup_datetime'].rstrip('Z'))
                            min_datetime = min(min_datetime, pickup_datetime)
                            max_datetime = max(max_datetime, pickup_datetime)
                        except (json.JSONDecodeError, KeyError):
                            pass

                except Exception as e:
                    print(f"Failed to process {key}: {e}")

    # Print the min and max pickup datetimes
    print(f"Minimum pickup datetime: {min_datetime}")
    print(f"Maximum pickup datetime: {max_datetime}")

# Usage
region = 'us-east-1'
bucket_name = 'aws-bigdata-blog'
folder_name = 'artifacts/flink-refarch/data/'

print_min_max_pickup_datetimes_from_s3(region, bucket_name, folder_name)
