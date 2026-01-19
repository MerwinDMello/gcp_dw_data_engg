create table if not exists `{{ params.param_hr_stage_dataset_name }}.zbdemohist`
(
  primary_fac STRING NOT NULL,
  hca_crt_date DATE NOT NULL,
  employee INT64 NOT NULL,
  process_level STRING NOT NULL,
  aux_status STRING,
  company INT64,
  emp_add_pr_dt DATE,
  emp_status STRING,
  hca_crt_time TIME,
  hca_crt_user STRING,
  hca_upd_date DATE,
  hca_upd_time TIME,
  hca_upd_user STRING,
  hire_date DATE,
  loa_pay_status STRING,
  norm_hours NUMERIC,
  pl_effect_date DATE,
  sip_demo_flag STRING,
  term_date DATE,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
