CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_contact_detail
(
  cn_patient_contact_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  contact_detail_measure_type_id INT64 NOT NULL OPTIONS(description='A unique identifier for each category corresponding to the contact.'),
  contact_detail_measure_value_text STRING OPTIONS(description='The result  for each category corresponding to the contact.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cn_patient_contact_sid, contact_detail_measure_type_id
OPTIONS(
  description='Contains detail communication data across different measures for a patient.'
);
