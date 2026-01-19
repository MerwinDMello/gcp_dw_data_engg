UPDATE {{params.param_hr_stage_dataset_name}}.performance_ratings_report
SET 
    employee_id = NULLIF(TRIM(employee_id),''),
    rev_period = NULLIF(TRIM(rev_period),''),
    /*rev_period_start_date = CASE
                        WHEN NULLIF(TRIM(rev_period_start_date),'') = '' THEN NULL
                        ELSE
                            PARSE_DATE('%Y-%m-%d', SUBSTR(rev_period_start_date, 1, 10))
                 END,
    rev_period_end_date = CASE
                        WHEN NULLIF(TRIM(rev_period_end_date),'') = '' THEN NULL
                        ELSE
                            PARSE_DATE('%Y-%m-%d', SUBSTR(rev_period_end_date, 1, 10))
                 END,*/
    rev_year = NULLIF(TRIM(rev_year),''),
    plan_name = NULLIF(TRIM(plan_name),''),
    eval_workflow_state = NULLIF(TRIM(eval_workflow_state),''),
    manager_record = NULLIF(TRIM(manager_record),''),
    manager_employee_id_record = NULLIF(TRIM(manager_employee_id_record),''),
    emp_perf_rat_num_val = NULLIF(TRIM(emp_perf_rat_num_val),''),
    emp_perf_rat_scale_val = NULLIF(TRIM(emp_perf_rat_scale_val),''),
    perf_rat_numeric_val = NULLIF(TRIM(perf_rat_numeric_val),''),
    perf_rat_scale_val = NULLIF(TRIM(perf_rat_scale_val),''),
    emp_smry_comp_rat_num_val = NULLIF(TRIM(emp_smry_comp_rat_num_val),''),
    emp_smry_comp_rat_scale_val = NULLIF(TRIM(emp_smry_comp_rat_scale_val),''),
    sumry_comp_rat_numeric_val = NULLIF(TRIM(sumry_comp_rat_numeric_val),''),
    sumry_comp_rat_scale_val = NULLIF(TRIM(sumry_comp_rat_scale_val),''),
    emp_smry_goal_rat_num_val = NULLIF(TRIM(emp_smry_goal_rat_num_val),''),
    emp_smry_goal_rat_scale_val = NULLIF(TRIM(REGEXP_REPLACE(emp_smry_goal_rat_scale_val, r'([^\x20-\x7E]+)', '')),''),
    smry_goal_rating_num_val = NULLIF(TRIM(smry_goal_rating_num_val),''),
    smry_goal_rat_scale_val = NULLIF(TRIM(REGEXP_REPLACE(smry_goal_rat_scale_val, r'([^\x20-\x7E]+)', '')),''),
    emp_strengths_accomplishments = NULLIF(TRIM(REGEXP_REPLACE(emp_strengths_accomplishments, r'([^\x20-\x7E]+)', '')),''),
    emp_area_improvement = NULLIF(TRIM(REGEXP_REPLACE(emp_area_improvement, r'([^\x20-\x7E]+)', '')),''),
    employee_additional_comments = NULLIF(TRIM(REGEXP_REPLACE(employee_additional_comments, r'([^\x20-\x7E]+)', '')),''),
    strengths_accomplishments = NULLIF(TRIM(REGEXP_REPLACE(strengths_accomplishments, r'([^\x20-\x7E]+)', '')),''),
    areas_improvement = NULLIF(TRIM(REGEXP_REPLACE(areas_improvement, r'([^\x20-\x7E]+)', '')),''),
    additional_comments = NULLIF(TRIM(REGEXP_REPLACE(additional_comments, r'([^\x20-\x7E]+)', '')),''),
    evaluation_record_id = NULLIF(TRIM(evaluation_record_id),''),
    job_code = NULLIF(TRIM(job_code),''),
    dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND)
WHERE dw_last_update_date_time IS NULL;