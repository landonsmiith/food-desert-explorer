import boto3
import os

def upload_file_to_s3(local_file, bucket_name, s3_key):
    s3 = boto3.client('s3')
    print(f"Uploading {local_file} to s3://{bucket_name}/{s3_key} ...")
    s3.upload_file(local_file, bucket_name, s3_key)
    print("Upload complete.")

if __name__ == "__main__":
    LOCAL_FILE = os.path.join("data", "raw", "FoodAccessAtlasData.xlsx")
    BUCKET_NAME = "fooddesertsources"
    S3_KEY = "FoodAccessAtlas.xlsx" 

    if os.path.exists(LOCAL_FILE):
        upload_file_to_s3(LOCAL_FILE, BUCKET_NAME, S3_KEY)
    else:
        print(f"File {LOCAL_FILE} not found. Please check the path.")
