import boto3
import os

BUCKET_NAME = "fooddesertsources"  
LOCAL_FOLDER = "data/raw/walkability.gdb"
S3_PREFIX = "raw/walkability.gdb/"

s3_client = boto3.client("s3")

for filename in os.listdir(LOCAL_FOLDER):
    local_path = os.path.join(LOCAL_FOLDER, filename)
    if os.path.isfile(local_path):
        s3_key = f"{S3_PREFIX}{filename}"
        try:
            s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
            print(f"Uploaded {local_path} to s3://{BUCKET_NAME}/{s3_key}")
        except Exception as e:
            print(f"Failed to upload {filename}: {e}")
