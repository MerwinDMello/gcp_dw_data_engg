CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_abstraction_measure
(
  abstraction_measure_sk INT64 NOT NULL OPTIONS(description='A unique identifier for each measure in abstraction'),
  abstraction_measure_name STRING NOT NULL OPTIONS(description='Type of measure in abstraction'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY abstraction_measure_sk
OPTIONS(
  description='Contains all the measures captured in abstraction'
);
