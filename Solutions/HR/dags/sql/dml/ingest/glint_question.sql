UPDATE {{ params.param_hr_stage_dataset_name }}.glint_question
SET dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND),
  question_text = NULLIF(TRIM(REGEXP_REPLACE(TRIM(question_text), r'([^\p{ASCII}]+)', '')),'')
WHERE 1 = 1;