CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.ref_tumor_site_diagnosis
(
  diagnosis_code STRING NOT NULL,
  diagnosis_type_code STRING NOT NULL,
  eff_from_date DATE,
  eff_to_date DATE,
  tumor_site_id INT64,
  detail_tumor_site_id INT64,
  tumor_type_id INT64,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY diagnosis_code, diagnosis_type_code;
