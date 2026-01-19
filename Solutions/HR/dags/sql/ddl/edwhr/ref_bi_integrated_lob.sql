CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_bi_integrated_lob
(
  integrated_lob_id INT64 NOT NULL,
  integrated_lob_desc STRING,
  sub_category_desc STRING,
  process_level_code STRING,
  dept_code STRING,
  corrected_dept_code STRING,
  skill_mix_desc STRING,
  omit_dept_code STRING,
  lob_code STRING,
  functional_dept_desc STRING,
  match_level_num INT64,
  recruitment_desc STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY integrated_lob_id;
