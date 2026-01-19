CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_biopsy
(
  cn_patient_biopsy_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  core_record_type_id INT64 OPTIONS(description='A unique identifier for each core record type.'),
  med_spcl_physician_id INT64 OPTIONS(description='A unique identifier for the medical specialist physician.'),
  referring_physician_id INT64 OPTIONS(description='A unique identifier for the referring physician.'),
  biopsy_type_id INT64 OPTIONS(description='A unique identifier for distinct biopsy type.'),
  biopsy_result_id INT64 OPTIONS(description='A unique identifier for distinct biopsy results'),
  biopsy_facility_id INT64 OPTIONS(description='The identifier corresponding the facility where the biopsy occurred.'),
  biopsy_site_location_id INT64 OPTIONS(description='The body location of where the biopsy took place.'),
  biopsy_physician_specialty_id INT64 OPTIONS(description='Specialty of physician who performed biopsy (gastroenterologist, interventional radiologist, primary care physician, radiologist, surgeon)'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  biopsy_date DATE OPTIONS(description='The date the biopsy occurred.'),
  biopsy_clip_sw INT64 OPTIONS(description='Indicates if a small metal clip was inserted during the biopsy to mark the biopsy location.'),
  biopsy_needle_sw INT64 OPTIONS(description='Indicates if a needle was used during the biopsy.'),
  general_biopsy_type_text STRING OPTIONS(description='A free text field to provide added detail to the patient biopsy.'),
  comment_text STRING OPTIONS(description='A free text field that adds insight into the module of where the comments was entered.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY core_record_type_id, nav_patient_id, tumor_type_id, diagnosis_result_id
OPTIONS(
  description='Contains the details behind a patients biopsy.'
);
