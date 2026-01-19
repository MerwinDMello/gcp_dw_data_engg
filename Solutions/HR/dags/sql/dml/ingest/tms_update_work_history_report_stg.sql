UPDATE {{params.param_hr_stage_dataset_name}}.work_history_report
SET work_history_description =  NULLIF(trim(REGEXP_REPLACE(TRIM(work_history_description), r'([^\x20-\x7E]+)', '')),''),
    dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND)
WHERE dw_last_update_date_time IS NULL;