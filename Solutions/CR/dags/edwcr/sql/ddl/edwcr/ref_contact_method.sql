CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_contact_method
(
  contact_method_id INT64 NOT NULL OPTIONS(description='A unique identifier for each contact method.'),
  contact_method_desc STRING OPTIONS(description='The description for each distinct contact method.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY contact_method_id
OPTIONS(
  description='Contains the distinct list of methods of contact that a navigator has.'
);
