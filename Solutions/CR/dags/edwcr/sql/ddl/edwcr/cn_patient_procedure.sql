CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_procedure
(
  cn_patient_procedure_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  core_record_type_id INT64 OPTIONS(description='A unique identifier for each core record type.'),
  procedure_type_id INT64 OPTIONS(description='A unique identifier for each procedure type.'),
  med_spcl_physician_id INT64 OPTIONS(description='A unique identifier for the medical specialist physician.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  procedure_date DATE OPTIONS(description='The date of the procedure.'),
  palliative_ind STRING OPTIONS(description='Indicates if the patient has a palliative condition.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY core_record_type_id, med_spcl_physician_id, nav_patient_id, tumor_type_id
OPTIONS(
  description='Contains all the procedures a patient underwent.'
);
