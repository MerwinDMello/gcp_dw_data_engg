CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cancer_patient_tumor_staging
(
  cancer_patient_tumor_staging_sk NUMERIC(29) NOT NULL OPTIONS(description='Unique key generated for each stage of cancer tumor for a patient'),
  cancer_patient_tumor_driver_sk NUMERIC(29) NOT NULL OPTIONS(description='A unique surrogate key generated for a patient and tumor record'),
  cancer_patient_driver_sk NUMERIC(29) NOT NULL OPTIONS(description='Unique patient key for consolidated patient records'),
  cancer_tumor_driver_sk INT64 NOT NULL OPTIONS(description='A unique identifier for each tumor record'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  best_cs_summary_desc STRING OPTIONS(description='Best SEER Summary stage that considers derived, SS2000 and SS1977 stage groups'),
  best_cs_tnm_desc STRING OPTIONS(description='Best stage that considers derived, pathologic and clinical AJCC stage groups'),
  tumor_size_num_text STRING OPTIONS(description='Text containing Tumor size '),
  cancer_stage_code STRING OPTIONS(description='Clinical or Pathalogical Cancer stage code'),
  cancer_stage_class_method_code STRING OPTIONS(description='This column tells if a cancer stage is classified using Clinical or Pathology method'),
  cancer_stage_type_code STRING OPTIONS(description='Stage type of cancer'),
  cancer_stage_result_text STRING OPTIONS(description='This column gives the details of a particular cancer stage'),
  ajcc_stage_desc STRING OPTIONS(description='Description for American Joint Committee On Cancer Stage'),
  tumor_size_summary_desc STRING OPTIONS(description='Identifier for Tumor size number summary'),
  source_system_code STRING NOT NULL OPTIONS(description='Unique value to identify the system from which data is sourced'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cancer_patient_tumor_staging_sk
OPTIONS(
  description='Contains tumor stage details for patient navigated by Sarah Cannon through Metriq iNavigate and Cancer Patient ID'
);
