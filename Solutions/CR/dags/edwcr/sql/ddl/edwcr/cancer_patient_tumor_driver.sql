CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cancer_patient_tumor_driver
(
  cancer_patient_tumor_driver_sk NUMERIC(29) NOT NULL OPTIONS(description='A unique surrogate key generated for a patient and tumor record'),
  cancer_patient_driver_sk NUMERIC(29) NOT NULL OPTIONS(description='Unique patient key for consolidated patient records'),
  cancer_tumor_driver_sk INT64 NOT NULL OPTIONS(description='A unique identifier for each tumor record'),
  coid STRING NOT NULL OPTIONS(description='A five character code for each facility.'),
  company_code STRING NOT NULL OPTIONS(description='Part of the unique identifier that identifies the company with which a facility is affiliated for application processing purposes.'),
  cr_patient_id INT64 OPTIONS(description='A unique identifier for each patient in Cancer Registry'),
  cn_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient in inavigate'),
  cp_patient_id NUMERIC(29) OPTIONS(description='A unique identifier for each patient in Cancer Patient ID system'),
  cr_tumor_primary_site_id INT64 OPTIONS(description='Identifier for primary tumor site from Cancer Registry system'),
  cn_tumor_type_id INT64 OPTIONS(description='Identifier for the tumor type (apart from general and Navque ) from cancer navigation system'),
  cp_icd_oncology_code STRING OPTIONS(description='The ICD Oncology code that classifies a neoplasm from Cancer Patient Id system'),
  source_system_code STRING NOT NULL OPTIONS(description='Unique value to identify the system from which data is sourced'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cancer_patient_tumor_driver_sk
OPTIONS(
  description='Contains consolidated patients navigated by Sarah Cannon through Metriq iNavigate and Cancer Patient ID and tumor information for them'
);
