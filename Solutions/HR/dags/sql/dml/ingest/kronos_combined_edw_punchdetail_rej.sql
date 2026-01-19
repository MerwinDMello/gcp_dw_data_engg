update {{params.param_hr_stage_dataset_name}}.kronos_combined_edw_punchdetail_rej
set kronos_clock_library = nullif(ltrim(kronos_clock_library),''),
    clock_code = nullif(ltrim(clock_code),''),
    approver_3_4 = nullif(ltrim(substring(approver_3_4,1,7)),''),
    pay_type = nullif(ltrim(pay_type),''),
    long_meal	= nullif(ltrim(long_meal),''),		
    other_dept = nullif(ltrim(other_dept),''),			
    out_of_ppd = nullif(ltrim(out_of_ppd),''),			
    short_meal = nullif(ltrim(short_meal),''),			
    department = nullif(ltrim(department),''),
    process_level = nullif(ltrim(substring(process_level,1,7)),''),
    posted_ind = nullif(ltrim(posted_ind),''),
    dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = "K"
where dw_last_update_date_time is null;