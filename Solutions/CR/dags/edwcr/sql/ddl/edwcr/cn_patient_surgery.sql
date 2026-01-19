CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_surgery
(
  cn_patient_surgery_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  surgery_side_id INT64 OPTIONS(description='Identifies which breast the surgery occured.'),
  surgery_facility_id INT64 OPTIONS(description='The facility where the surgery occured.'),
  surgery_type_id INT64 OPTIONS(description='A unique identifier for each surgery type.'),
  core_record_type_id INT64 OPTIONS(description='A unique identifier for each core record type.'),
  med_spcl_physician_id INT64 OPTIONS(description='A unique identifier for the medical specialist physician.'),
  referring_physician_id INT64 OPTIONS(description='A unique identifier for the referring physician.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  surgery_date DATE OPTIONS(description='The date of the surgery.'),
  general_surgery_type_text STRING OPTIONS(description='The type of surgery performed in free text form.'),
  reconstructive_offered_ind STRING OPTIONS(description='Indicates if reconstructive surgery was offered.'),
  palliative_ind STRING OPTIONS(description='Indicates if the patient has a palliative condition.'),
  comment_text STRING OPTIONS(description='A free text field that adds insight into the module of where the comments was entered.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY core_record_type_id, nav_patient_id, tumor_type_id, diagnosis_result_id
OPTIONS(
  description='Contains the surgical details associated with a patients treatment. Results from the treatment are stored in CN_Patient_Procedure_Pathology_Result.'
);
