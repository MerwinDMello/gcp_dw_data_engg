update {{params.param_hr_stage_dataset_name}}.ref_clock_stg
set 
kronos_clock_library = TRIM(kronos_clock_library),
clock_code = TRIM(clock_code),		
clock_desc = TRIM(clock_desc),
dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'),SECOND), 
source_system_code = "K"
where dw_last_update_date_time is null;