CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_plan_purpose
(
  plan_purpose_sk INT64 NOT NULL OPTIONS(description='Unique surrogate key generated for a radiation oncology plan purpose in EDW'),
  plan_purpose_name STRING OPTIONS(description='Name of the plan purpose'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY plan_purpose_sk
OPTIONS(
  description='Contains information for radiation oncology plan purpose'
);
