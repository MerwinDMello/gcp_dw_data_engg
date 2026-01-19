create table if not exists `{{ params.param_hr_stage_dataset_name }}.zxauxref`
(
  acct_unit STRING NOT NULL,
  company INT64 NOT NULL,
  hca_coid STRING,
  hca_crt_date DATE,
  hca_crt_time TIME,
  hca_crt_user STRING,
  hca_dept INT64,
  hca_hr_dept INT64,
  hca_pl_coid STRING,
  hca_pr_branch STRING,
  hca_unit STRING,
  hca_upd_date DATE,
  hca_upd_time TIME,
  hca_upd_user STRING,
  hr_company INT64,
  process_level STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
