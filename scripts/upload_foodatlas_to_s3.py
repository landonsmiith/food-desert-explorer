import boto3
import os

# --- CONFIGURATION ---
BUCKET_NAME = "fooddesertsources"   
LOCAL_FOLDER = "data/raw"         
S3_PREFIX = "raw/"                

# --- INITIALIZE S3 CLIENT ---
s3_client = boto3.client("s3")

# --- UPLOAD ALL FOOD ATLAS FILES ---
for filename in os.listdir(LOCAL_FOLDER):
    if filename.startswith("Food"):
        local_path = os.path.join(LOCAL_FOLDER, filename)
        s3_key = f"{S3_PREFIX}{filename}"
        try:
            s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
            print(f"Uploaded {local_path} to s3://{BUCKET_NAME}/{s3_key}")
        except Exception as e:
            print(f"Failed to upload {filename}: {e}")
