UPDATE
  {{ params.param_hr_audit_dataset_name }}.audit_control
SET
  tgt_tbl_nm = REGEXP_EXTRACT(tgt_tbl_nm, r'^[a-zA-Z-]+\.([a-zA-Z0-9_.]+$)')
WHERE
  src_sys_nm = 'enwisen'
  AND audit_type = 'RECORD_COUNT';