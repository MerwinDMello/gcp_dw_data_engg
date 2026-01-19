CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_lookup_name_stg
(
  lookup_sid INT64 NOT NULL OPTIONS(description='A unique surrogate key for each lookup column'),
  lookup_name STRING OPTIONS(description='Name of the column for which lookup is done'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.')
)
OPTIONS(
  description='This table contains name of the column for which lookup is done'
);
