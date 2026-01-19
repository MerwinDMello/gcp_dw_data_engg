CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_icd_oncology
(
  icd_oncology_code STRING NOT NULL OPTIONS(description='The ICD Oncology code that classifies a neoplasm.'),
  icd_oncology_type_code STRING NOT NULL OPTIONS(description='Identifies the version of ICD Oncology codes the code refers to.'),
  icd_oncology_category_type_code STRING NOT NULL OPTIONS(description='Identifies which axes or coding system the code corresponds to. Topographical vs Morphological'),
  icd_oncology_site_desc STRING OPTIONS(description='The site description for each ICD Oncology Code'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY icd_oncology_code, icd_oncology_type_code, icd_oncology_category_type_code
OPTIONS(
  description='The International Classification of Diseases for Oncology (ICD-O) has been internationally recognized as the definitive classification of neoplasms. It is used by cancer registries throughout the world to record incidence of malignancy and survival rates.'
);
