import boto3
import os
import io
import snappy

def download_and_decompress_files_from_s3(region, bucket_name, folder_name, local_directory):
    s3_client = boto3.client('s3', region_name=region)
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    if 'Contents' in objects:
        for obj in objects['Contents']:
            key = obj['Key']
            if not key.endswith('/'): 
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
                s3_data = s3_object['Body'].read()
                snappyfile = io.BytesIO(s3_data)
                decompressed_data = io.BytesIO()
                snappy.stream_decompress(src=snappyfile, dst=decompressed_data)
                decompressed_data.seek(0)  
                local_file_path = os.path.join(local_directory, key.split('/')[-1].replace('.snz', ''))  
                with open(local_file_path, 'wb') as f:
                    f.write(decompressed_data.read())
                print(f"Downloaded and decompressed {key} to {local_file_path}")
    else:
        print("No files found in the specified folder.")

Usage
region = 'us-east-1'
bucket_name = 'aws-bigdata-blog'
folder_name = 'artifacts/flink-refarch/data/nyc-tlc-trips.snz/'
local_directory = r'C:\Projects\Newyork_city_taxi_dataproject\Snappy-Files'

download_and_decompress_files_from_s3(region, bucket_name, folder_name, local_directory)
