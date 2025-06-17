import boto3
import os

def download_foodaccessatlas_file(bucket_name, prefix, local_dir):
    """
    Download Food Access Atlas Excel file from a given S3 bucket and prefix to a local directory.
    Args:
        bucket_name (str): Name of the S3 bucket.
        prefix (str): S3 prefix/folder (e.g., 'raw/').
        local_dir (str): Local directory to save files.
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    for obj in bucket.objects.filter(Prefix=prefix):
        filename = os.path.basename(obj.key)
        if filename == "FoodAccessAtlasData.xlsx":
            local_path = os.path.join(local_dir, filename)
            print(f"Downloading {obj.key} to {local_path}")
            bucket.download_file(obj.key, local_path)

if __name__ == "__main__":
    BUCKET_NAME = "fooddesertsources"      
    PREFIX = "raw/"                
    LOCAL_DIR = "data/processed/"  # Save to processed directory

    download_foodaccessatlas_file(BUCKET_NAME, PREFIX, LOCAL_DIR)
