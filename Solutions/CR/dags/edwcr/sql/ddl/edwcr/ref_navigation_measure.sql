CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_navigation_measure
(
  nav_measure_id INT64 NOT NULL OPTIONS(description='A unique identifier for each measure in inavigate'),
  nav_measure_type STRING NOT NULL OPTIONS(description='Type of measure in inavigate i.e. Contact Detail ,Core Adherence,Family History Query\rPathology Result , Patient Test Type'),
  nav_measure_name STRING NOT NULL OPTIONS(description='The name of the measure'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
OPTIONS(
  description='Contains all the measures captured in inavigate'
);
