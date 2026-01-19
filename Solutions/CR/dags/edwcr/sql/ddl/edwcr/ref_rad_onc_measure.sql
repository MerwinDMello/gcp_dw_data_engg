CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_rad_onc_measure
(
  rad_onc_measure_id INT64 NOT NULL OPTIONS(description='A unique identifier for each measure in radiation oncology'),
  rad_onc_measure_type STRING NOT NULL OPTIONS(description='Type of measure in radiation oncology\rPathology Result , Patient Test Type'),
  rad_onc_measure_name STRING NOT NULL OPTIONS(description='The name of the measure'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY rad_onc_measure_id
OPTIONS(
  description='Contains all the measures captured in radiation oncology'
);
