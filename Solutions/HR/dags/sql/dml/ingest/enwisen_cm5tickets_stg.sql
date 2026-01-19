UPDATE {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg
SET dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND),
  subject = REPLACE(subject,CHR(8211), CHR(45))
WHERE 1 = 1;