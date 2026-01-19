CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_hospital
(
  hospital_id INT64 NOT NULL OPTIONS(description='A unique identifier for hospital'),
  hospital_code STRING OPTIONS(description='A unique code for a hospital'),
  hospital_name STRING OPTIONS(description='Name of the hospital'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY hospital_id
OPTIONS(
  description='This table contains hospital code and name'
);
