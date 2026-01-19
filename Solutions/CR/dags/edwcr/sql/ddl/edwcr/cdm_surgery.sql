CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cdm_surgery
(
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Primary Index element of the Patient Encounter in EDW.  It is accessed by a lookup using the natural key Patient Account Number and Facility Identifier.'),
  surgical_case_seq STRING NOT NULL OPTIONS(description='Part of natural key of the Case'),
  activity_date DATE NOT NULL OPTIONS(description='Part of natural key of the Case'),
  coid STRING NOT NULL OPTIONS(description='Unique identifier for the enterprise'),
  company_code STRING NOT NULL OPTIONS(description='An Identifier for the enterprise entity.  e.g. "H" for "HCA"'),
  pat_acct_num NUMERIC(29) NOT NULL OPTIONS(description='A unique number assigned by the hospital to the patient at time of registration.'),
  primary_surgeon_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Primary Surgeon for the Procedure'),
  surgeon_npi NUMERIC(29) OPTIONS(description='A unique identifier assigned to the healthcare provider by CMS.'),
  physician_name STRING OPTIONS(description='The full name of the HCP as listed in the source system.'),
  surgeon_start_date_id DATE OPTIONS(description='Date when the procedure begins (Cut or SORST)'),
  surgeon_start_time INT64 OPTIONS(description='Time when the procedure begins (Cut or SORST)'),
  surgeon_end_date_id DATE OPTIONS(description='Date when procedure ends (Close or SOREN)'),
  surgeon_end_time INT64 OPTIONS(description='Time when procedure end (Close or SOREN)'),
  procedure_desc STRING OPTIONS(description='Description of the surgical procedure'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Contains information regarding each patients surgery.'
);
