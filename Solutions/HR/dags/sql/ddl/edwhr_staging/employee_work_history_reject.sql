CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.employee_work_history_reject
(
  employee_id STRING,
  work_history_company_name STRING,
  work_history_job_title STRING,
  work_history_description STRING,
  work_history_start_date STRING,
  work_history_end_date STRING,
  work_history_address STRING,
  work_history_city STRING,
  work_history_region STRING,
  work_history_country STRING,
  work_history_postal_code STRING,
  work_history_id STRING,
  dw_last_update_date_time DATETIME NOT NULL,
  reject_reason STRING,
  reject_stg_tbl_nm STRING
);
