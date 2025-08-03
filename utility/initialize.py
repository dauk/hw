%sql
CREATE SCHEMA IF NOT EXISTS dwh;
#Running queries below will truncate tables if they already exist
CREATE OR REPLACE TABLE dim_vendor (
  vendor_key BIGINT GENERATED ALWAYS AS IDENTITY,
  vendor_name STRING
);


CREATE OR REPLACE TABLE dim_category (
  category_key BIGINT GENERATED ALWAYS AS IDENTITY,
  category STRING
);

CREATE OR REPLACE TABLE obt_hosts (
  date date,
  host STRING,
  category STRING,
  vendor STRING
);

CREATE OR REPLACE TABLE dim_host (
  host_key BIGINT GENERATED ALWAYS AS IDENTITY,
  host STRING
);

CREATE OR REPLACE TABLE fact_hosts (
  date date,
  host_key bigint,
  category_key bigint,
  vendor_key bigint
);

CREATE OR REPLACE TABLE ops_raw_load (
  raw_load_key BIGINT GENERATED ALWAYS AS IDENTITY,
  ts timestamp,
  is_success BOOLEAN,
  file_source string,
  file_destination string
);

CREATE OR REPLACE TABLE ops_stage_load (
  stage_load_key BIGINT GENERATED ALWAYS AS IDENTITY,
  ts timestamp,
  is_success BOOLEAN,
  param_str string,
  error_message string
);
