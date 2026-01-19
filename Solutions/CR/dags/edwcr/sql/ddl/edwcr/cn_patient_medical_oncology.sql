CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_medical_oncology
(
  cn_patient_medical_oncology_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  treatment_type_id INT64 OPTIONS(description='A unique identifier for each treatment type.'),
  medical_oncology_facility_id INT64 OPTIONS(description='The facility identifier for where the medical oncology was given.'),
  core_record_type_id INT64 OPTIONS(description='A unique identifier for each core record type.'),
  med_spcl_physician_id INT64 OPTIONS(description='A unique identifier for the medical specialist physician.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  diagnosis_result_id INT64 OPTIONS(description='A unique identifier for each diagnosis result.'),
  nav_diagnosis_id INT64 OPTIONS(description='A unique identfier for each diagnosis type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  core_record_date DATE OPTIONS(description='The date the core record was updated.'),
  treatment_start_date DATE OPTIONS(description='The start date of the treatment.'),
  treatment_end_date DATE OPTIONS(description='The date the treatment ended.'),
  estimated_end_date DATE OPTIONS(description='Estimated end date for the medical oncology.'),
  drug_name STRING OPTIONS(description='The name of the drug given.'),
  dose_dense_chemo_ind STRING OPTIONS(description='Indicates if the therapy included dose-dense chemotherapy.'),
  drug_dose_amt_text STRING OPTIONS(description='A free text field that identifies the amount given.'),
  drug_dose_measurement_text STRING OPTIONS(description='The measurement text for the dose.'),
  drug_available_ind STRING OPTIONS(description='Indicates if the drug was vailable.'),
  drug_qty INT64 OPTIONS(description='The quantity of the drug given for each cycle.'),
  cycle_num INT64 OPTIONS(description='The number of cycles for which the medical oncology was given.'),
  cycle_frequency_text STRING OPTIONS(description='How often the medical oncology was given.'),
  medical_oncology_reason_text STRING OPTIONS(description='The reason the medical oncology was given.'),
  terminated_ind STRING OPTIONS(description='Indicates if the medical oncology was terminiated.'),
  treatment_therapy_schedule_code STRING OPTIONS(description='Indicates if the therapy was given before the main threatment or removal. (Neoadjuvant vs adjuvant)'),
  comment_text STRING OPTIONS(description='A free text field that adds insight into the module of where the comments was entered.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY tumor_type_id, diagnosis_result_id, nav_diagnosis_id, navigator_id
OPTIONS(
  description='Contains the details behind the medical oncology treatment for a patient.'
);
