UPDATE {{ params.param_hr_audit_dataset_name }}.audit_control
SET tgt_tbl_nm =  CONCAT('edwhr.',tgt_tbl_nm)
where SPLIT(tgt_tbl_nm, '.')[SAFE_OFFSET(1)] is null and audit_type = 'VALIDATION_COUNT';