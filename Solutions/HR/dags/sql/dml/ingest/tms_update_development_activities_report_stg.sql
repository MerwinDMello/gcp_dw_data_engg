UPDATE {{params.param_hr_stage_dataset_name}}.development_activities_report
SET employee_id = NULLIF(TRIM(employee_id),''),
    activity_competency_name =  COALESCE(TRIM(REGEXP_REPLACE(activity_competency_name, r'([^\x20-\x7E]+)', '')),''),
    description =  COALESCE(TRIM(REGEXP_REPLACE(description, r'([^\x20-\x7E]+)', '')),''),
    catalog_activity_name =  COALESCE(TRIM(REGEXP_REPLACE(catalog_activity_name, r'([^\x20-\x7E]+)', '')),''),
    catalog_activity_description =  COALESCE(TRIM(REGEXP_REPLACE(catalog_activity_description, r'([^\x20-\x7E]+)', '')),''),
    priority = NULLIF(TRIM(priority),''),
    status = NULLIF(TRIM(status),''),
    /*start_date = CASE
                        WHEN NULLIF(TRIM(start_date),'') = '' THEN NULL
                        ELSE
                            PARSE_DATE('%Y-%m-%d', SUBSTR(start_date, 1, 10))
                 END,
    end_date = CASE
                        WHEN NULLIF(TRIM(start_date),'') = '' THEN NULL
                        ELSE
                            PARSE_DATE('%Y-%m-%d', SUBSTR(end_date, 1, 10))
                 END,*/
    hours = COALESCE(TRIM(hours),''),
    development_goals_notes =  COALESCE(TRIM(REGEXP_REPLACE(development_goals_notes, r'([^\x20-\x7E]+)', '')),''),
    development_activity_record_id = NULLIF(TRIM(development_activity_record_id),''),
    job_code = NULLIF(TRIM(job_code),''),
    dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND)
    WHERE  dw_last_update_date_time IS NULL;