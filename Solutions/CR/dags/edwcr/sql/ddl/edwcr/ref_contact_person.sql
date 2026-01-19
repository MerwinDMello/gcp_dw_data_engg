CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_contact_person
(
  contact_person_id INT64 NOT NULL OPTIONS(description='A unique identifier for each distinct person contacted.'),
  contact_person_desc STRING OPTIONS(description='The description of distinct people contacted.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY contact_person_id
OPTIONS(
  description='Contains the distinct list of people a navigator contacted associated with a patient communication.'
);
