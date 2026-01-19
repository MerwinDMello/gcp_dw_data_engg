CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_radiation_oncology
(
  radiation_id INT64 NOT NULL OPTIONS(description='A unique identifier for radiation'),
  treatment_id INT64 OPTIONS(description='Unique identifier for a treatment'),
  radiation_type_id INT64 OPTIONS(description='A unique identifier for type of radiation treatment'),
  radiation_hospital_id INT64 OPTIONS(description='Hospital identifier for a radiation'),
  radiation_treatment_start_date DATE OPTIONS(description='Treatment start date for patient'),
  radiation_treatment_end_date DATE OPTIONS(description='Treatment end date for patient'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY radiation_id
OPTIONS(
  description='This table contains radiation oncology data for the patient'
);
