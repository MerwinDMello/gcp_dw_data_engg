UPDATE {{params.param_hr_stage_dataset_name}}.competency_ratings_report
SET 
    evaluation_workflow_state = NULLIF(TRIM(evaluation_workflow_state),''),
    employee_rating_scale_value = NULLIF(TRIM(employee_rating_scale_value),''),
    competency = NULLIF(TRIM(competency),''),
    competency_group = NULLIF(TRIM(competency_group),''),
    manager_rating_scale_value = NULLIF(TRIM(manager_rating_scale_value),''),
    comp_record_id = NULLIF(TRIM(comp_record_id),''),
    dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND)
WHERE dw_last_update_date_time IS NULL;