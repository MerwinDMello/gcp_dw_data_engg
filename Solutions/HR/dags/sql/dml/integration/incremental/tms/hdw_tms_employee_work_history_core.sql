  BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = datetime_trunc(current_datetime('US/Central'), SECOND);

  
  BEGIN TRANSACTION;
  

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_work_history AS tgt SET valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND), dw_last_update_date_time = stg.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.employee_work_history_wrk AS stg WHERE tgt.employee_work_history_sid = stg.employee_work_history_sid
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
   AND (trim(CAST(coalesce(stg.employee_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.employee_sid, -999) as STRING))
   OR trim(CAST(coalesce(stg.employee_num, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_num, -999) as STRING))
   OR trim(CAST(coalesce(stg.employee_talent_profile_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.employee_talent_profile_sid, -999) as STRING))
   OR trim(CAST(coalesce(stg.previous_work_address_sid, -999) as STRING)) <> trim(CAST(coalesce(tgt.previous_work_address_sid, -999) as STRING))
   OR trim(coalesce(stg.work_history_company_name, '')) <> trim(coalesce(tgt.work_history_company_name, ''))
   OR trim(coalesce(stg.work_history_job_title_text, '')) <> trim(coalesce(tgt.work_history_job_title_text, ''))
   OR trim(coalesce(stg.work_history_desc, '')) <> trim(coalesce(tgt.work_history_desc, ''))
   OR coalesce(stg.work_history_start_date, DATE '9999-01-01') <> coalesce(tgt.work_history_start_date, DATE '9999-01-01')
   OR coalesce(stg.work_history_end_date, DATE '9999-01-01') <> coalesce(tgt.work_history_end_date, DATE '9999-01-01')
   OR trim(CAST(coalesce(stg.lawson_company_num, -999) as STRING)) <> trim(CAST(coalesce(tgt.lawson_company_num, -999) as STRING))
   OR trim(coalesce(stg.process_level_code, '')) <> trim(coalesce(tgt.process_level_code, '')));

  INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_work_history (employee_work_history_sid, valid_from_date, employee_sid, employee_num, employee_talent_profile_sid, previous_work_address_sid, work_history_company_name, work_history_job_title_text, work_history_desc, work_history_start_date, work_history_end_date, lawson_company_num, process_level_code, valid_to_date, source_system_key, source_system_code, dw_last_update_date_time)
    SELECT
        stg.employee_work_history_sid,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS valid_from_date,
        stg.employee_sid,
        stg.employee_num,
        stg.employee_talent_profile_sid,
        stg.previous_work_address_sid,
        TRIM(stg.work_history_company_name),
        TRIM(stg.work_history_job_title_text),
        TRIM(stg.work_history_desc),
        stg.work_history_start_date,
        stg.work_history_end_date,
        stg.lawson_company_num,
        stg.process_level_code,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.source_system_key,
        stg.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_work_history_wrk AS stg
      WHERE ( coalesce(stg.employee_sid,0),
              coalesce(stg.employee_num,0), 
              coalesce(trim(CAST(stg.employee_talent_profile_sid as STRING)),''), 
              coalesce(trim(CAST(stg.employee_work_history_sid as STRING)),''),  
              coalesce(trim(CAST(stg.previous_work_address_sid as STRING)),''),  
              coalesce(trim(stg.work_history_company_name),''),  
              coalesce(trim(stg.work_history_job_title_text),''),  
              coalesce(trim(stg.work_history_desc),''),  
              coalesce(trim(CAST(stg.work_history_start_date as STRING)),''),  
              coalesce(trim(CAST(stg.work_history_end_date as STRING)),''),  
              coalesce(trim(CAST(stg.lawson_company_num as STRING)),''),  
              coalesce(trim(stg.process_level_code),''),  
              coalesce(trim(stg.source_system_key),'')
            ) 
            NOT IN
            (
            SELECT AS STRUCT
            coalesce(tgt.employee_sid,0),
            coalesce(tgt.employee_num,0),
            coalesce(trim(CAST(tgt.employee_talent_profile_sid as STRING)),''), 
            coalesce(trim(CAST(tgt.employee_work_history_sid as STRING)),''), 
            coalesce(trim(CAST(tgt.previous_work_address_sid as STRING)),''), 
            coalesce(trim(tgt.work_history_company_name),''), 
            coalesce(trim(tgt.work_history_job_title_text),''), 
            coalesce(trim(tgt.work_history_desc),''), 
            coalesce(trim(CAST(tgt.work_history_start_date as STRING)),''), 
            coalesce(trim(CAST(tgt.work_history_end_date as STRING)),''), 
            coalesce(trim(CAST(tgt.lawson_company_num as STRING)),''), 
            coalesce(trim(tgt.process_level_code),''), 
            coalesce(trim(tgt.source_system_key),'')
          FROM
            {{ params.param_hr_base_views_dataset_name }}.employee_work_history AS tgt
          WHERE tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
      )
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
  ;

      SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT Employee_Work_History_SID ,valid_from_date
      FROM {{ params.param_hr_core_dataset_name }}.employee_work_history
      GROUP BY Employee_Work_History_SID ,valid_from_date
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.employee_work_history ');
  ELSE 
  COMMIT TRANSACTION;
  END IF;


  UPDATE {{ params.param_hr_core_dataset_name }}.employee_work_history AS tgt SET valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND), dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHERE tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
   AND tgt.employee_work_history_sid NOT IN(
    SELECT
        employee_work_history_wrk.employee_work_history_sid
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_work_history_wrk
      GROUP BY 1
  );
END
