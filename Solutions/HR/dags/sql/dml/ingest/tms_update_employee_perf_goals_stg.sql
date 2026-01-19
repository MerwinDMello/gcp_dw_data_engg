UPDATE {{params.param_hr_stage_dataset_name}}.employee_perf_goals
SET 
goal_title = NULLIF(TRIM(REGEXP_REPLACE(goal_title, r'([^\x20-\x7E]+)', '')),''),
emp_goal_rating = NULLIF(TRIM(REGEXP_REPLACE(emp_goal_rating, r'([^\x20-\x7E]+)', '')),''),
mgr_goal_rating = NULLIF(TRIM(REGEXP_REPLACE(mgr_goal_rating, r'([^\x20-\x7E]+)', '')),''),
plan_name = NULLIF(TRIM(REGEXP_REPLACE(plan_name, r'([^\x20-\x7E]+)', '')),''),
expected_result = NULLIF(TRIM(REGEXP_REPLACE(expected_result, r'([^\x20-\x7E]+)', '')),''),
measure = NULLIF(TRIM(REGEXP_REPLACE(measure, r'([^\x20-\x7E]+)', '')),''),
Goal_Status = NULLIF(TRIM(Goal_Status),''),
Goal_Progress = NULLIF(TRIM(Goal_Progress),''),
job_code = NULLIF(TRIM(job_code),''),
dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND)
WHERE dw_last_update_date_time IS NULL;