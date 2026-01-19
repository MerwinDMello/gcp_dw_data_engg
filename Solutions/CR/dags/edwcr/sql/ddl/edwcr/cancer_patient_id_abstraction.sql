CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cancer_patient_id_abstraction
(
  cancer_abstraction_sk INT64 NOT NULL OPTIONS(description='A unique identifier for each output record.'),
  cancer_patient_id_output_sk INT64 NOT NULL OPTIONS(description='A unique identifier for each output record.'),
  message_control_id_text STRING OPTIONS(description='Unique id for the message provided from EDW'),
  coid STRING OPTIONS(description='A five character code for each facility.'),
  company_code STRING OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  patient_dw_id NUMERIC(29) OPTIONS(description='An internal EDW identifier for each encounter.'),
  pat_acct_num NUMERIC(29) OPTIONS(description='Account Number for the message'),
  abstraction_report_assigned_date_time DATETIME OPTIONS(description='The datetime the abstraction report was assigned to the user.'),
  abstraction_date_time DATETIME OPTIONS(description='The datetime data abstraction was done'),
  abstraction_action_user_3_4_id STRING OPTIONS(description='Identification of the user taking action in the abstraction application in the 3-4 ID format.'),
  abstraction_action_desc STRING OPTIONS(description='User selection from controlled list; Confirm, False Positive, Insufficient Details'),
  abstraction_action_date_time DATETIME OPTIONS(description='The datetime the action was taken by the user.'),
  primary_icd_oncology_code STRING OPTIONS(description='ICD-O-3 that corresponds to the primary abstraction'),
  primary_icd_site_desc STRING OPTIONS(description='The site description for each ICD Oncology Code of primary abstraction'),
  primary_icd_site_and_model_score_desc STRING OPTIONS(description='The site and model score description for each ICD Oncology Code of primary abstraction.'),
  changed_primary_icd_oncology_code STRING OPTIONS(description='ICD-O-3 that corresponds to the changed primary abstraction'),
  changed_primary_icd_site_desc STRING OPTIONS(description='The site description for each ICD Oncology Code of changed primary abstraction'),
  topography_icd_oncology_code STRING OPTIONS(description='ICD-O-3 that corresponds to the topography'),
  topography_icd_site_desc STRING OPTIONS(description='The site description for each ICD Oncology Code of topography'),
  laterality_icd_oncology_code STRING OPTIONS(description='ICD-O-3 that corresponds to the laterality abstraction'),
  laterality_icd_site_desc STRING OPTIONS(description='The site description for each ICD Oncology Code of laterality abstraction'),
  secondary_icd_oncology_code STRING OPTIONS(description='ICD-O-3 that corresponds to the secondary abstraction'),
  secondary_icd_site_desc STRING OPTIONS(description='The site description for each ICD Oncology Code of secondary abstraction'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY patient_dw_id
OPTIONS(
  description='This table contains the abstraction data of the cancer patient identifier model on HDFS.'
);
