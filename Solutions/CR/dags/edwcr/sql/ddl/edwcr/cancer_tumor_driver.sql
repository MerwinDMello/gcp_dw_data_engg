CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cancer_tumor_driver
(
  cancer_tumor_driver_sk INT64 NOT NULL OPTIONS(description='Unique key generated for a cancer tumor record'),
  cp_icd_oncology_code STRING OPTIONS(description='The ICD Oncology code that classifies a neoplasm from Cancer Patient Id system'),
  cp_icd_oncology_site_desc STRING OPTIONS(description='The site description for each ICD Oncology Code from Cancer Patient Id system'),
  cp_icd_oncology_group_name STRING OPTIONS(description='The site group name for each ICD Oncology Code from Cancer Patient Id system'),
  cr_tumor_primary_site_id INT64 OPTIONS(description='Identifier for primary tumor site from Cancer Registry system'),
  cn_tumor_type_id INT64 OPTIONS(description='Identifier for the tumor type (apart from general and Navque ) from cancer navigation system '),
  cn_general_tumor_type_id INT64 OPTIONS(description='Identifier for the general tumor type from cancer navigation system'),
  cn_navque_tumor_type_id INT64 OPTIONS(description='Identifier for the Navque tumor type from cancer navigation system'),
  cr_icd_oncology_code STRING OPTIONS(description='The  International Classification Of Disease Oncology code for primary site from Cancer Registry system'),
  cr_icd_oncology_site_desc STRING OPTIONS(description='Description for the  International Classification Of Disease Oncology code for primary site from Cancer Registry system'),
  cn_tumor_group_name STRING OPTIONS(description='Group name of the tumor from cancer navigation system'),
  cn_tumor_type_desc STRING OPTIONS(description='Description of the tumor type from cancer navigation system'),
  cn_general_tumor_group_name STRING OPTIONS(description='Group name for general tumor from cancer navigation system'),
  cn_general_tumor_type_desc STRING OPTIONS(description='Description for general tumor type from cancer navigation system'),
  cn_navque_tumor_type_desc STRING OPTIONS(description='Description for NavQue tumor type from cancer navigation system'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cancer_tumor_driver_sk
OPTIONS(
  description='Master tumor table which stores tumor types (International Classification of Diseases for Oncology ) from cancer registry , cancer patient id and cancer navigation system'
);
