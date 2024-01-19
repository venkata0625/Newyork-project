import boto3
import os
import io
import bz2

def download_and_decompress_files_from_s3(region, bucket_name, folder_name, local_directory):
    # Create an S3 client
    s3_client = boto3.client('s3', region_name=region)

    # Check if the local directory exists, if not, create it
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    # List objects within the specified folder
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    if 'Contents' in objects:
        for obj in objects['Contents']:
             key = obj['Key']
            # Process only .bz2 files
             if key.endswith('.bz2'):  
                try:
                    # Get the object from S3
                    s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
                    s3_data = s3_object['Body'].read()
                    

                    # Decompress the data using bz2
                    decompressed_data = bz2.decompress(s3_data)
                    
                    # Construct the local file path
                    local_file_path = os.path.join(local_directory, key.split('/')[-1].replace('.bz2', ''))

                    # Write the decompressed data to a file
                    with open(local_file_path, 'wb') as f:
                        f.write(decompressed_data)
                    print(f"Downloaded and decompressed {key} to {local_file_path}")

                except OSError as e:
                    print(f"Failed to decompress {key}: {e}")
    else:
        print("No files found in the specified folder.")

# Usage
region = 'us-east-1'
bucket_name = 'aws-bigdata-blog'
folder_name = 'artifacts/flink-refarch/data/'
local_directory = r'C:\Projects\Newyork_city_taxi_dataproject\BZ2-Files'

download_and_decompress_files_from_s3(region, bucket_name, folder_name, local_directory)
