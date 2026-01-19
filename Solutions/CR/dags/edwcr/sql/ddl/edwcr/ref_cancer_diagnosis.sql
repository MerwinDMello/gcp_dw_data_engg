CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_cancer_diagnosis
(
  diagnosis_code STRING NOT NULL,
  diagnosis_type_code STRING NOT NULL,
  diagnosis_desc STRING,
  diagnosis_formatted_code STRING,
  comorbidity_sw INT64,
  eff_from_date DATE,
  eff_to_date DATE,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY diagnosis_code, diagnosis_type_code;
