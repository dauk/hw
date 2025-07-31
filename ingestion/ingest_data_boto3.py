import boto3
from datetime import datetime
import os
import sys
sys.path.append("/Workspace/Users/dauuuk@gmail.com/hw/utility")
from utils import get_source_config, get_sink_config

SOURCE_ACCESS_KEY, SOURCE_SECRET_ACCESS_KEY, SOURCE_REGION = get_source_config()
SINK_ACCESS_KEY, SINK_SECRET_ACCESS_KEY, SINK_REGION = get_sink_config()

def ingest_data(
    source_bucket_name: str, source_prefix: str, sink_bucket_name: str, sink_prefix: str
) -> None:
    source_s3_resource = boto3.resource(
        "s3",
        aws_access_key_id=SINK_ACCESS_KEY,
        aws_secret_access_key=SINK_SECRET_ACCESS_KEY,
        region_name=SINK_REGION,
    )
    sink_s3_client = boto3.client(
        "s3",
        aws_access_key_id=SOURCE_ACCESS_KEY,
        aws_secret_access_key=SOURCE_SECRET_ACCESS_KEY,
        region_name=SOURCE_REGION,
    )

    # Creating file path based on current date
    date_str = datetime.now().strftime("%Y-%m-%d")
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    sink_path = f"{sink_prefix}/{date_str}"

    bucket = source_s3_resource.Bucket(source_bucket_name)

    for obj in bucket.objects.filter(Prefix=source_prefix):
        source_key = obj.key
        # Skip directories and empty files
        if source_key.endswith('/') or obj.size == 0:
            continue

        # Extract original filename and extension
        filename = os.path.basename(source_key)
        name, ext = os.path.splitext(filename)

        # Append timestamp to filename
        new_filename = f"{name}_{timestamp_str}{ext}"

        destination_key = f"{sink_path}/{new_filename}"

        copy_source = {"Bucket": source_bucket_name, "Key": source_key}
        sink_s3_client.copy_object(
            CopySource=copy_source, Bucket=sink_bucket_name, Key=destination_key
        )

        print(f"Copied {source_key} to {destination_key}")


# TODO move copied files to processed
