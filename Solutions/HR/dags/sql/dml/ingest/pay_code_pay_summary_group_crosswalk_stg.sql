update {{params.param_hr_stage_dataset_name}}.pay_code_pay_summary_group_crosswalk_stg
set	kronos_clock_library = nullif(trim(kronos_clock_library),''),			
    pay_code = nullif(trim(pay_code),''),			
    pay_code_desc = nullif(trim(pay_code_desc),''),			
    kronos_payroll_interface_code = nullif(trim(kronos_payroll_interface_code),''),			
    pay_summary_group = nullif(trim(pay_summary_group),''),			
    source_system_code = 'K',			
    dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND)
where dw_last_update_date_time is null;