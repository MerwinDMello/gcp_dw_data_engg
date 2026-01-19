CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cr_patient_date_driver
(
  cancer_patient_driver_sk NUMERIC(29) NOT NULL OPTIONS(description='Unique patient key for consolidated patient records'),
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Primary Index element of the Patient Encounter in EDW.  It is accessed by a lookup using the natural key Patient Account Number and Facility Identifier.'),
  cancer_diagnosis_date DATE OPTIONS(description='The date a patient was diagnosed with cancer according to their Electronic Medical Record. Sourced from Sarah Cannon.'),
  cancer_diagnosis_90_day_prior_date DATE OPTIONS(description='The date 90 days prior to a patients cancer diagnosis date.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='This table is a bridge between Sarah Cannon Patient Identifiers and Clinical Data Management (CDM) Patient Identifiers.'
);
