CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_insurance
(
  cr_patient_id INT64 NOT NULL OPTIONS(description='A unique identifier for each patient.'),
  tumor_id INT64 NOT NULL OPTIONS(description='Unique identifier of tumor'),
  insurance_type_id INT64 OPTIONS(description='Unique identifier of Insurance type used for tumor treatment'),
  insurance_company_id INT64 OPTIONS(description='Unique identifier of Insurance company paid for tumor treatment'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cr_patient_id, tumor_id
OPTIONS(
  description='This table contains insurance details used by patient'
);
