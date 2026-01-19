CREATE OR REPLACE VIEW `{{ params.param_pbs_audit_dataset_name }}.audit_control_combined`
AS
SELECT
  'Parallon' AS domain,
  'PBS' AS lob,
  uuid,
  table_id,
  src_sys_nm,
  src_tbl_nm,
  tgt_tbl_nm,
  audit_type,
  expected_value,
  actual_value,
  ROUND(AVG(actual_value) OVER (PARTITION BY tgt_tbl_nm ORDER BY audit_time ASC ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),2) AS avg_14_run,
  load_start_time,
  load_end_time,
  load_run_time,
  job_name,
  audit_time,
  audit_status
FROM
   `{{ params.param_pbs_audit_dataset_name }}.audit_control`;