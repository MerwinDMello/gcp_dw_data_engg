CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.icd_oncology_diagnosis_code_xwalk
(
  icd_oncology_code STRING NOT NULL OPTIONS(description='The ICD Oncology code that classifies a neoplasm.'),
  icd_oncology_type_code STRING NOT NULL OPTIONS(description='Identifies the version of ICD Oncology codes the code refers to.'),
  icd_oncology_category_type_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  diagnosis_code STRING NOT NULL OPTIONS(description='The diagnosis code which indicates that a patients condition is complicated.  This is a standard ICD-9-CM code used to identify the patients diagnosis at discharge.  Up to 15 occurrences of diagnosis codes are maintained with Diag_Rank_Num.  '),
  diagnosis_type_code STRING NOT NULL OPTIONS(description='Code to identify a diagnosis as an ICD9 or other types'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY icd_oncology_code, icd_oncology_type_code, icd_oncology_category_type_code, diagnosis_code
OPTIONS(
  description='Crosswalks ICD Oncology Codes to ICD10 Diagnosis Codes.'
);
