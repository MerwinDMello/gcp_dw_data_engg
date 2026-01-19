CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_treatment
(
  treatment_id INT64 NOT NULL OPTIONS(description='Unique identifier for a treatment'),
  tumor_id INT64 OPTIONS(description='Unique identifier of tumor'),
  treatment_hospital_id INT64 OPTIONS(description='Hospital identifier for treatment'),
  treatment_type_id INT64 OPTIONS(description='A unique identifier for type of treatment performed'),
  surgical_site_id INT64 OPTIONS(description='Anotomical location of surgical procedure'),
  surgical_margin_result_id INT64 OPTIONS(description='Result for surgical margins'),
  treatment_type_group_id INT64 OPTIONS(description='Unique Identifier for representation of the treatment performed'),
  clinical_trial_start_date DATE OPTIONS(description='Date clinical trial started'),
  treatment_start_date DATE OPTIONS(description='Date Treatment or Procedure was performed'),
  clinical_trial_text STRING OPTIONS(description='Text containing clinical trial name'),
  comment_text STRING OPTIONS(description='Comment free text'),
  treatment_performing_physician_code STRING OPTIONS(description='Physician who performed the Treatment or Procedure'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY treatment_id
OPTIONS(
  description='This table contains treatment information for the patient'
);
