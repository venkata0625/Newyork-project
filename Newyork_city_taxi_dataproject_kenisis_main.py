import boto3
import bz2
import json
import argparse

def check_or_create_kinesis_stream(kinesis_client, stream_name):
        kinesis_client.describe_stream(StreamName=stream_name)
        print(f"Stream '{stream_name}' exists.")
   
def process_and_send_to_kinesis(bucket_name, folder_name, stream_name):
    s3_client = boto3.client('s3')
    kinesis_client = boto3.client('kinesis')

    check_or_create_kinesis_stream(kinesis_client, stream_name)

    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    if 'Contents' in objects:
        for obj in objects['Contents']:
           key = obj['Key']
           if key.endswith('.bz2'):
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
            s3_data = s3_object['Body'].read()
            
            # Decompress the data
            decompressed_data = bz2.decompress(s3_data)

            for line in decompressed_data.splitlines():
                try:
                    json_data = json.loads(line)
                    print(f"{json_data}\n" )
                    kinesis_client.put_record(
                        StreamName=stream_name,
                        Data=json.dumps(json_data),
                        PartitionKey=str(json_data.get('trip_id', 'default'))
                    )
                    print(f"Processed and sent data from {key} to Kinesis")
                   
                except Exception as e:
                    print(f"Error processing line: {e}")

            
    else:
      print("No files found in the specified folder.")

parser = argparse.ArgumentParser(description='Your script description.')
parser.add_argument('--bucket-name', help='Input file')
parser.add_argument('--object-prefix',help='Output file')
parser.add_argument('--kinesis-stream-name', help='Increase output verbosity')

args = parser.parse_args()


process_and_send_to_kinesis(args.bucket_name, args.object_prefix, args.kinesis_stream_name)       