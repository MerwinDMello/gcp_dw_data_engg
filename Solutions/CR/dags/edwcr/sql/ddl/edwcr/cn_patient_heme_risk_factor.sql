CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_heme_risk_factor
(
  cn_patient_heme_diagnosis_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  previous_tumor_site_id INT64 OPTIONS(description='Indication of previous tumor site or disease the patient had'),
  other_previous_tumor_site_id INT64 OPTIONS(description='Indication of other previous tumor site or disease the patient had'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  risk_factor_text STRING OPTIONS(description='Text for risk factor linked to patient haematology'),
  other_risk_factor_text STRING OPTIONS(description='Text for other risk factors linked to patient haematology'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id
OPTIONS(
  description='Contains risk factors for Hematology patient'
);
