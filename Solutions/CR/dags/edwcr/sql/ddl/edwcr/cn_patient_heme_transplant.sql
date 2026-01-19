CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_heme_transplant
(
  cn_patient_heme_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  transplant_type_id INT64 OPTIONS(description='Indicates the type of transplant record\r'),
  cellular_therapy_status_id INT64 OPTIONS(description='Indicates the status value of the patient transplant-cellular therapy\r'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  med_spcl_physician_id INT64 OPTIONS(description='Indicates physician associated with transplant record'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  cellular_therapy_status_date DATE OPTIONS(description='Date associated to the patient\'s transplant-cellular therapy status'),
  cellular_therapy_comment_text STRING OPTIONS(description='Cellular therapy navigator free text comments\r'),
  transfer_date DATE OPTIONS(description='Date patient was transferred to BMT coordinator'),
  transplant_date DATE OPTIONS(description='Date of transplant record '),
  transplant_comment_text STRING OPTIONS(description='Transplant free text navigator comments\r'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id
OPTIONS(
  description='Contains treatment regimen details for Hematology patient'
);
