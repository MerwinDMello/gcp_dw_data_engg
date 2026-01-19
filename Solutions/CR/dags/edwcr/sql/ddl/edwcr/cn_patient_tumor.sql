CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_tumor
(
  cn_patient_tumor_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  referral_source_facility_id INT64 OPTIONS(description='The facility the referral was provided from.'),
  nav_status_id INT64 OPTIONS(description='A unique identifier for each result.'),
  treatment_end_physician_id INT64 OPTIONS(description='The physician that ended the treatment.'),
  treatment_end_facility_id INT64 OPTIONS(description='The facility where the treatment ended.'),
  treatment_end_reason_text STRING OPTIONS(description='The reason the treatment was ended.'),
  nav_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient.'),
  tumor_type_id INT64 OPTIONS(description='A unique identifier for each tumor type.'),
  navigator_id INT64 OPTIONS(description='A unique identifier for each navigator.'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  electronic_folder_id_text STRING OPTIONS(description='A unique identifier tied to the source.'),
  identification_period_text STRING OPTIONS(description='The time period of when the tumor was identified.'),
  referral_date DATE OPTIONS(description='Date of the referral.'),
  referring_physician_id INT64 OPTIONS(description='A unique identifier for the referring physician.'),
  nav_end_reason_text STRING OPTIONS(description='The reason the navigation ended of the patient.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY nav_patient_id, tumor_type_id, navigator_id, coid
OPTIONS(
  description='Contains details around the discovery of a tumor and patient.'
);
