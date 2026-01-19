CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_facility
(
  facility_id INT64 NOT NULL OPTIONS(description='A unique identifier for each facility.'),
  facility_name STRING OPTIONS(description='The name of the facility.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY facility_id
OPTIONS(
  description='Contains the list of facilities for different event types - Imaging, Biopsy, Treatment, Oncology, etc...'
);
