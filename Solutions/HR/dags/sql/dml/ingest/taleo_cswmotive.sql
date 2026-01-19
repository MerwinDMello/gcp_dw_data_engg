update  {{params.param_hr_stage_dataset_name}}.taleo_cswmotive
set	mnemonic = nullif(trim(mnemonic),''),			
    description = nullif(trim(description),''),			
    name = nullif(trim(name),''),
    source_system_code='{{params.param_source_system_code}}',
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND)
where dw_last_update_date_time is null;