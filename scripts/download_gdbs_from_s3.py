import boto3
import os
import errno

def assert_dir_exists(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def download_gdb_from_s3(bucket_name, gdb_prefix, local_dir):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=gdb_prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('/'):
                continue  # Skip pseudo-folder
            rel_path = os.path.relpath(key, gdb_prefix)
            local_path = os.path.join(local_dir, rel_path)
            assert_dir_exists(os.path.dirname(local_path))
            print(f"Attempting download: {key} → {local_path}")
            try:
                s3.download_file(bucket_name, key, local_path)
                print(f"SUCCESS: Downloaded {key} to {local_path}")
            except Exception as e:
                print(f"FAILED: {key} to {local_path} — {e}")

if __name__ == "__main__":
    BUCKET_NAME = "fooddesertsources"
    for gdb_name in ["smartlocation.gdb", "walkability.gdb"]:
        PREFIX = f"raw/{gdb_name}/"
        LOCAL_DIR = f"data/processed/{gdb_name}/"
        print(f"\nProcessing {PREFIX} → {LOCAL_DIR}")
        download_gdb_from_s3(BUCKET_NAME, PREFIX, LOCAL_DIR)
