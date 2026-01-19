CREATE OR REPLACE VIEW `{{ params.param_hr_audit_dataset_name }}.audit_control_combined`
AS
SELECT
  'HRG' AS domain,
  'HRG' AS lob,
  uuid,
  table_id,
  src_sys_nm,
  src_tbl_nm,
  tgt_tbl_nm,
  CASE audit_type
    WHEN 'RECORD_COUNT' THEN 'RECORD_COUNT'
    WHEN 'INGESTION'  THEN 'RECORD_COUNT'
    WHEN 'NO_VALIDATION_SQL' THEN 'NO_VALIDATION_SQL'
    WHEN 'VALIDATION_COUNT' THEN 'RECORD_COUNT'
  END
  AS audit_type,
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
   `{{ params.param_hr_audit_dataset_name }}.audit_control_lawson`

UNION ALL

SELECT
  'HRG' AS domain,
  'HRG' AS lob,
  uuid,
  table_id,
  src_sys_nm,
  src_tbl_nm,
  tgt_tbl_nm,
  CASE audit_type
    WHEN 'RECORD_COUNT' THEN 'INGEST_CNTRLID_2'
    WHEN 'INGESTION'  THEN 'INGEST_CNTRLID_2'
    WHEN 'NO_VALIDATION_SQL' THEN 'NO_VALIDATION_SQL'
    WHEN 'VALIDATION_COUNT' THEN 'VALIDATION_CNTRLID_2'
  END
  AS audit_type,
  control2_expected_value as expected_value,
  control2_actual_value as actual_value,
  ROUND(AVG(control2_actual_value) OVER (PARTITION BY tgt_tbl_nm ORDER BY audit_time ASC ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),2) AS avg_14_run,
  load_start_time,
  load_end_time,
  load_run_time,
  job_name,
  audit_time,
  audit_status
FROM
   `{{ params.param_hr_audit_dataset_name }}.audit_control_lawson`
WHERE control2_expected_value <> 0 OR control2_actual_value <> 0

UNION ALL

SELECT
  'HRG' AS domain,
  'HRG' AS lob,
  uuid,
  table_id,
  src_sys_nm,
  src_tbl_nm,
  tgt_tbl_nm,
  CASE audit_type
    WHEN 'RECORD_COUNT' THEN 'INGEST_CNTRLID_3'
    WHEN 'INGESTION'  THEN 'INGEST_CNTRLID_3'
    WHEN 'NO_VALIDATION_SQL' THEN 'NO_VALIDATION_SQL'
    WHEN 'VALIDATION_COUNT' THEN 'VALIDATION_CNTRLID_3'
  END
  AS audit_type,
  control3_expected_value as expected_value,
  control3_actual_value as actual_value,
  ROUND(AVG(control3_actual_value) OVER (PARTITION BY tgt_tbl_nm ORDER BY audit_time ASC ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),2) AS avg_14_run,
  load_start_time,
  load_end_time,
  load_run_time,
  job_name,
  audit_time,
  audit_status
FROM
   `{{ params.param_hr_audit_dataset_name }}.audit_control_lawson`
WHERE control3_expected_value <> 0 OR control3_actual_value <> 0

UNION ALL

SELECT
  'HRG' AS domain,
  'HRG' AS lob,
  uuid,
  table_id,
  src_sys_nm,
  src_tbl_nm,
  tgt_tbl_nm,
  CASE audit_type
    WHEN 'RECORD_COUNT' THEN 'RECORD_COUNT'
    WHEN 'INGESTION'  THEN 'RECORD_COUNT'
    WHEN 'NO_VALIDATION_SQL' THEN 'NO_VALIDATION_SQL'
    WHEN 'VALIDATION_COUNT' THEN 'RECORD_COUNT'
  END
  AS audit_type,
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
   `{{ params.param_hr_audit_dataset_name }}.audit_control`