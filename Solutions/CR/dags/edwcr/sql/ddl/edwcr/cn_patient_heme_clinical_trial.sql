CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_heme_clinical_trial
(
  cn_patient_heme_clinical_trial_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record for a Hematology clinical trial'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  clinical_trial_evaluated_ind STRING OPTIONS(description='Indication if Clinical Trial was evaluated (Heme)'),
  clinical_trial_evaluated_date DATE OPTIONS(description='Date the patient was evaluated'),
  clinical_trial_enrolled_ind STRING OPTIONS(description='Indicates if the patient was enrolled in the clinical trial.'),
  clinical_trial_enrolled_date DATE OPTIONS(description='The date the patient was enrolled in the clinical trial.'),
  clinical_trial_offered_ind STRING OPTIONS(description='Indicates if the patient was offered the trial.'),
  clinical_trial_offered_date DATE OPTIONS(description='The date the clinical trial was offered from.'),
  clinical_trial_not_offered_text STRING OPTIONS(description='Description of why the trial was not offered'),
  clinical_trial_not_offered_other_text STRING OPTIONS(description='Other reason why the trial was not offered'),
  clinical_trial_name STRING OPTIONS(description='Name of the clinical trial'),
  clinical_trial_other_name STRING OPTIONS(description='Other name of the clinical trial.'),
  not_screened_reason_text STRING OPTIONS(description='Text if the patient was not screened'),
  not_screened_other_reason_text STRING OPTIONS(description='Other reason why the patient was not screened'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, tumor_type_id, diagnosis_result_id, nav_diagnosis_id
OPTIONS(
  description='Contains details for Hematology patient clinical trial'
);
