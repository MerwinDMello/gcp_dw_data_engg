CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cdm_ed_visit
(
  patient_dw_id NUMERIC(29) NOT NULL OPTIONS(description='Primary Index element of the Patient Encounter in EDW.  It is accessed by a lookup using the natural key Patient Account Number and Facility Identifier.'),
  coid STRING NOT NULL OPTIONS(description='Unique identifier for the enterprise'),
  company_code STRING NOT NULL OPTIONS(description='An Identifier for the enterprise entity.  e.g. "H" for "HCA"'),
  pat_acct_num NUMERIC(29) NOT NULL OPTIONS(description='A unique number assigned by the hospital to the patient at time of registration.'),
  reason_for_visit_text STRING OPTIONS(description='This is the reason for the patient to come to the ED'),
  chief_complaint_query_mnemonic_cs STRING OPTIONS(description='This is the query mnemonic for the chief complaint of the patient\'s visit.'),
  ed_complaint_desc STRING OPTIONS(description='The Desription associated with the Complaint code in the Dictionary'),
  arrival_date_time DATETIME OPTIONS(description='Date of ED Admission/The date and time a patient first arrives to ED'),
  depart_date_time DATETIME OPTIONS(description='This is when the patient physically left the ED'),
  admit_date_time DATETIME OPTIONS(description='Date Admitted from ED TO Hospital/The date time that someone becomes an inpatient'),
  admit_ind STRING OPTIONS(description='Admitted from ED to Hospital (Indicator)'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='Emergency department data at the patient level.'
);
