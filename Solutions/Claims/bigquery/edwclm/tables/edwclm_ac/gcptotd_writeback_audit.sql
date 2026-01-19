CREATE TABLE IF NOT EXISTS {{ params.param_clm_audit_dataset_name }}.gcptotd_writeback_audit
(
  uuid STRING,
  srctableid INT64,
  bqtablename STRING,
  tdttablename STRING,
  load_type STRING,
  audit_type STRING,
  expected_value INT64,
  actual_value INT64,
  source_table_count INT64,
  target_table_count INT64,
  load_start_time DATETIME,
  load_end_time DATETIME,
  load_run_time STRING,
  audit_time DATETIME,
  audit_status STRING
);