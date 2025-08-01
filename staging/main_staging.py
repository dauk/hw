import yaml

from process_bronze_data import process_data

with open("/Workspace/Users/dauuuk@gmail.com/hw/staging/stage_config.yaml", "r") as f:
    stage_config = yaml.safe_load(f)

def run_stage_ingestion(stage_dict: dict):
    for category, vendors in stage_dict.items():
        for vendor, config in vendors.items():
            process_data(
                source_bucket_name = config["source_bucket_name"],
                source_prefix = config["source_prefix"],
                sink_bucket_name = config["sink_bucket_name"],
                sink_prefix = config["sink_prefix"],
                data_type = config["data_type"],
                days_to_load = config["days_to_load"],
                starts_with = config["starts_with"],
                regex_clear = config["regex_clear"]
            )

if __name__ == "__main__":
    run_stage_ingestion(stage_config)