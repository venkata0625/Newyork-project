import boto3
import os
import io
import bz2

def download_and_decompress_files_from_s3(region, bucket_name, folder_name, local_directory):
    s3_client = boto3.client('s3', region_name=region)
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    if 'Contents' in objects:
        for obj in objects['Contents']:
             key = obj['Key']
             if key.endswith('.bz2'):  
                try:
                    s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
                    s3_data = s3_object['Body'].read()
                    decompressed_data = bz2.decompress(s3_data)
                    local_file_path = os.path.join(local_directory, key.split('/')[-1].replace('.bz2', ''))
                    with open(local_file_path, 'wb') as f:
                        f.write(decompressed_data)
                    print(f"Downloaded and decompressed {key} to {local_file_path}")
                except OSError as e:
                    print(f"Failed to decompress {key}: {e}")
    else:
        print("No files found in the specified folder.")
region = 'us-east-1'
bucket_name = 'aws-bigdata-blog'
folder_name = 'artifacts/flink-refarch/data/'
local_directory = r'C:\Projects\Newyork_city_taxi_dataproject\BZ2-Files'

download_and_decompress_files_from_s3(region, bucket_name, folder_name, local_directory)
