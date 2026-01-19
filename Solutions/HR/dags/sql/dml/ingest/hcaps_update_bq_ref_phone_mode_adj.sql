update {{ params.param_hr_stage_dataset_name }}.ref_phone_mode_adjustment
set measure_id_text = trim(measure_id_text),
   --eff_to_date	= '12/31/9999'-,		
    dw_last_update_date_time = timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND),
    source_system_code = "H"
where dw_last_update_date_time is null;