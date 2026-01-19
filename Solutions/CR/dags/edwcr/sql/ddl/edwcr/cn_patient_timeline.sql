CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_timeline
(
  cn_patient_timeline_id INT64 NOT NULL OPTIONS(description='A unique identifier for each record for timeline of the patient'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  nav_referred_date DATE OPTIONS(description='Date patient was referred for navigation'),
  first_treatment_date DATE OPTIONS(description='First treatment date for the patient'),
  first_consult_date DATE OPTIONS(description='First consult date for the patient'),
  first_imaging_date DATE OPTIONS(description='First imaging date for the patient'),
  first_medical_oncology_date DATE OPTIONS(description='Date of first medical oncology for patient'),
  first_radiation_oncology_date DATE OPTIONS(description='Date of first radiation oncology for patient'),
  first_diagnosis_date DATE OPTIONS(description='Date patient was first diagnosed'),
  first_biopsy_date DATE OPTIONS(description='Date of first biopsy for the patient'),
  first_surgery_consult_date DATE OPTIONS(description='Date patient consulted first time for surgery'),
  first_surgery_date DATE OPTIONS(description='Date of first surgery for the patient'),
  survivorship_care_plan_close_date DATE OPTIONS(description='Date of survivorship care plan closure'),
  survivorship_care_plan_resolve_date DATE OPTIONS(description='Date of survivorship care plan resolution.'),
  end_treatment_date DATE OPTIONS(description='Date for end treatment for the patient'),
  death_date DATE OPTIONS(description='Death date for the patient'),
  diagnosis_first_treatment_day_num INT64 OPTIONS(description='Number of days from diagnosis date to first treatment date'),
  diagnosis_first_treatment_available_ind STRING OPTIONS(description='Indicates if diagnosis to first treatment days is available.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cn_patient_timeline_id
OPTIONS(
  description='Contains details of timeline for the patient'
);
