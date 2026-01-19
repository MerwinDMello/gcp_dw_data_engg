CREATE TABLE IF NOT EXISTS {{ params.param_clm_audit_dataset_name }}.audit_control
(
  uuid STRING,
  table_id INT64,
  src_sys_nm STRING,
  src_tbl_nm STRING,
  tgt_tbl_nm STRING,
  audit_type STRING,
  expected_value NUMERIC,
  actual_value NUMERIC,
  load_start_time DATETIME,
  load_end_time DATETIME,
  load_run_time STRING,
  job_name STRING,
  audit_time DATETIME,
  audit_status STRING
);