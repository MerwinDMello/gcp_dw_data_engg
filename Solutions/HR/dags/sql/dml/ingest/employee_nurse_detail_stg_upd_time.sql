update {{params.param_hr_stage_dataset_name}}.employee_nurse_detail_stg
set				
    dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND)
where dw_last_update_date_time is null;