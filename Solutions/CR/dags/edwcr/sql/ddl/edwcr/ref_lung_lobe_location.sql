CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_lung_lobe_location
(
  lung_lobe_location_id INT64 NOT NULL OPTIONS(description='A unique identifier for the quadrant of the lung the radiation occured.'),
  lung_lobe_location_desc STRING OPTIONS(description='The quadrant of the lung.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY lung_lobe_location_id
OPTIONS(
  description='Contains the different lung lobe locations in a distinct list.'
);
