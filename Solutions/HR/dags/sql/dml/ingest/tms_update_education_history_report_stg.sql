UPDATE {{ params.param_hr_stage_dataset_name }}.education_history_report
SET 
    employee_id = NULLIF(TRIM(employee_id),''),
    school_name = NULLIF(TRIM(school_name),''),
    school_type = NULLIF(TRIM(school_type),''),
    degree = NULLIF(TRIM(degree),''),
    major = NULLIF(TRIM(major),''),
    education_start_date = NULLIF(TRIM(education_start_date),''),
    education_end_date = NULLIF(TRIM(education_end_date),''),
    year_graduated = NULLIF(TRIM(year_graduated),''),
    gpa = NULLIF(TRIM(gpa),''),
    education_comments = NULLIF(TRIM(education_comments),''),
    edu_hist_record_id = NULLIF(TRIM(edu_hist_record_id),''),
    job_code = NULLIF(TRIM(job_code),''),
    dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND)
WHERE dw_last_update_date_time IS NULL;