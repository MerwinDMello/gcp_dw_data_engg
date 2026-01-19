CREATE OR REPLACE VIEW
  `{{ params.param_cr_audit_dataset_name }}.audit_control_combined` AS
SELECT
  'OPS' AS domain,
  'CR' AS lob,
  uuid,
  table_id,
  src_sys_nm,
  src_tbl_nm,
  tgt_tbl_nm,
  CASE audit_type
    WHEN 'RECORD_COUNT' THEN 'RECORD_COUNT'
    WHEN 'INGESTION' THEN 'RECORD_COUNT'
    WHEN 'NO_VALIDATION_SQL' THEN 'NO_VALIDATION_SQL'
    WHEN 'VALIDATION_COUNT' THEN 'RECORD_COUNT'
END
  AS audit_type,
  expected_value AS expected_value,
  actual_value AS actual_value,
  ROUND(AVG(actual_value) OVER (PARTITION BY tgt_tbl_nm ORDER BY audit_time ASC ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),2) AS avg_14_run,
  load_start_time,
  load_end_time,
  load_run_time,
  job_name,
  audit_time,
  audit_status
FROM
  `{{ params.param_cr_audit_dataset_name }}.audit_control`;