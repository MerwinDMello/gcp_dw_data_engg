update {{params.param_hr_stage_dataset_name}}.time_entry_pay_code_detail_stg
set	
    hr_company = coalesce(hr_company,0),
    kronos_pay_code = nullif(ltrim(kronos_pay_code),''),
    pay_summary_group = nullif(ltrim(pay_summary_group),''),
    process_level = ltrim(process_level),		
    source_system_code = 'K',			
    dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND)
where dw_last_update_date_time is null;