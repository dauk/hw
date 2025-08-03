import yaml
import sys

from ingest_data_boto3 import ingest_data

with open("/Workspace/Users/dauuuk@gmail.com/hw/ingestion/s3_ingest_config.yaml", "r") as f:
    s3_ingestion = yaml.safe_load(f)


def run_data_ingestion(ingestion_dict : dict) -> None:
    for category, vendors in ingestion_dict.items():
        for vendor, config in vendors.items():
            ingest_data(
                source_bucket_name=config["source_bucket_name"],
                source_prefix=config["source_prefix"],
                sink_bucket_name=config["sink_bucket_name"],
                sink_prefix=config["sink_prefix"])


if __name__ == "__main__":
    run_data_ingestion(s3_ingestion)
