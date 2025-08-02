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