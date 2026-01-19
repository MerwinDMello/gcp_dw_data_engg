CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_referring_physician
(
  referring_physician_id INT64 NOT NULL OPTIONS(description='A unique identifier for each referring physician.'),
  referring_physician_name STRING OPTIONS(description='Name of the referring physician'),
  navigation_referred_ind STRING OPTIONS(description='Indicator if referred for navigation.'),
  biopsy_referred_ind STRING OPTIONS(description='Indicator if referred for biopsy.'),
  surgery_referred_ind STRING OPTIONS(description='Indicator if referred for surgery.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY referring_physician_id
OPTIONS(
  description='Contains the details of referring physicians'
);
