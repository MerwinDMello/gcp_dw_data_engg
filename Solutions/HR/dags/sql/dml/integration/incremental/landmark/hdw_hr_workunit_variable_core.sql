BEGIN

DECLARE current_ts datetime;
SET current_ts = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

  UPDATE {{ params.param_hr_core_dataset_name }}.hr_workunit_variable AS tgt 
  SET valid_to_date = current_ts - INTERVAL 1 SECOND, 
  dw_last_update_date_time = stg.dw_last_update_date_time, active_dw_ind = 'N' FROM {{ params.param_hr_stage_dataset_name }}.hr_workunit_variable_wrk AS stg WHERE tgt.workunit_sid = stg.workunit_sid
   AND tgt.variable_seq_num = stg.variable_seq_num
   AND tgt.variable_name = stg.variable_name
   AND (coalesce(tgt.workunit_num, -999) <> coalesce(stg.workunit_num, -999)
   OR coalesce(tgt.variable_type_num, -999) <> coalesce(stg.variable_type_num, -999)
   OR coalesce(trim(tgt.variable_value_text), 'X') <> coalesce(trim(stg.variable_value_text), 'X')
   OR coalesce(tgt.lawson_company_num, -999) <> coalesce(stg.lawson_company_num, -999)
   OR coalesce(trim(tgt.process_level_code), 'X') <> coalesce(trim(stg.process_level_code), 'X')
   OR coalesce(trim(tgt.source_system_code), 'X') <> coalesce(trim(stg.source_system_code), 'X')
   OR coalesce(trim(tgt.active_dw_ind), 'X') <> coalesce(trim(stg.active_dw_ind), 'X'))
   AND tgt.valid_to_date = datetime '9999-12-31 23:59:59';

  INSERT INTO {{ params.param_hr_core_dataset_name }}.hr_workunit_variable (workunit_sid, variable_seq_num, valid_from_date, valid_to_date, workunit_num, variable_name, variable_type_num, variable_value_text, lawson_company_num, process_level_code, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        stg.workunit_sid,
        stg.variable_seq_num,
        stg.valid_from_date,
        stg.valid_to_date,
        stg.workunit_num,
        stg.variable_name,
        stg.variable_type_num,
        stg.variable_value_text,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.active_dw_ind,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hr_workunit_variable_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable AS tgt ON stg.workunit_sid = tgt.workunit_sid
         AND tgt.variable_seq_num = stg.variable_seq_num
         AND coalesce(tgt.workunit_num, -999) = coalesce(stg.workunit_num, -999)
         AND coalesce(trim(tgt.variable_name), 'X') = coalesce(trim(stg.variable_name), 'X')
         AND coalesce(tgt.variable_type_num, -999) = coalesce(stg.variable_type_num, -999)
         AND coalesce(trim(tgt.variable_value_text), 'X') = coalesce(trim(stg.variable_value_text), 'X')
         AND coalesce(tgt.lawson_company_num, -999) = coalesce(stg.lawson_company_num, -999)
         AND coalesce(trim(tgt.process_level_code), 'X') = coalesce(trim(stg.process_level_code), 'X')
         AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
         AND coalesce(trim(tgt.active_dw_ind), 'X') = coalesce(trim(stg.active_dw_ind), 'X')
         AND tgt.valid_to_date = datetime '9999-12-31 23:59:59'
      WHERE tgt.workunit_sid IS NULL
  ;
END;