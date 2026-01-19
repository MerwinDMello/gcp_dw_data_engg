CREATE OR REPLACE VIEW
  edwops_ac.audit_control_combined AS
SELECT
  *
FROM
  edwpi_ac.audit_control_combined
UNION ALL
SELECT
  DISTINCT 'OPS' AS Domain,
  'ASD' AS LOB,
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
  edwasd_ac.audit_control
UNION ALL
SELECT
  DISTINCT 'OPS' AS Domain,
  'GA' AS LOB,
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
  edwga_ac.audit_control
UNION ALL
SELECT
  *
FROM
  edwcp_ac.audit_control_combined
UNION ALL
SELECT
  *
FROM
  edwcr_ac.audit_control_combined
