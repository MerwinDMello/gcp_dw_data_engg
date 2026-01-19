CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_radiation_oncology
(
  cn_patient_radiation_oncology_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  treatment_site_location_id INT64 OPTIONS(description='The location of the treatment.'),
  treatment_type_id INT64 OPTIONS(description='A unique identifier for each treatment type.'),
  lung_lobe_location_id INT64 OPTIONS(description='A unique identifier for the quadrant of the lung the radiation occured.'),
  radiation_oncology_facility_id INT64 OPTIONS(description='The facility that gave the patient radiation therapy.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  core_record_type_id INT64 OPTIONS(description='A unique identifier for each core record type.'),
  med_spcl_physician_id INT64 OPTIONS(description='A unique identifier for the medical specialist physician.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  core_record_date DATE OPTIONS(description='The date the core record was updated.'),
  treatment_start_date DATE OPTIONS(description='The start date of the treatment.'),
  treatment_end_date DATE OPTIONS(description='The date the treatment ended.'),
  treatment_fractions_num NUMERIC(31, 2) OPTIONS(description='Number of doses or fractions the patient will receive'),
  elapse_ind STRING OPTIONS(description='Indicates if there was an elapse during the radiation treatment.'),
  elapse_start_date DATE OPTIONS(description='Start date of the elapse.'),
  elapse_end_date DATE OPTIONS(description='End date of the elapse.'),
  radiation_oncology_reason_text STRING OPTIONS(description='The reason radiation oncology was provided.'),
  palliative_ind STRING OPTIONS(description='Indicates if the patient has a palliative condition.'),
  treatment_therapy_schedule_code STRING OPTIONS(description='Indicates if the therapy was given before the main threatment or removal. (Neoadjuvant vs adjuvant)'),
  comment_text STRING OPTIONS(description='A free text field that adds insight into the module of where the comments was entered.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id
OPTIONS(
  description='Contains the details behind the radiation oncology treatment'
);
