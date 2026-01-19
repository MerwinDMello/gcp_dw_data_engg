BEGIN
  DECLARE dup_count INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = datetime_trunc(current_datetime('US/Central'), SECOND);

  BEGIN TRANSACTION;
  UPDATE {{ params.param_hr_core_dataset_name }}.fact_employee_availability AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = stg.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.fact_employee_availability_wrk AS stg WHERE tgt.employee_talent_profile_sid = stg.employee_talent_profile_sid
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
   AND (trim(CAST(coalesce(stg.employee_sid, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_sid, -999) as STRING))
   OR trim(CAST(coalesce(stg.employee_num, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_num, -999) as STRING))
   OR trim(CAST(coalesce(stg.jobs_pooled_for_cnt, -999) as STRING)) <> trim(CAST(coalesce(tgt.jobs_pooled_for_cnt, -999) as STRING))
   OR trim(CAST(coalesce(stg.employee_talent_pool_cnt, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_talent_pool_cnt, -999) as STRING))
   OR trim(CAST(coalesce(stg.employee_successor_cnt, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_successor_cnt, -999) as STRING))
   OR trim(CAST(coalesce(stg.employee_ready_now_cnt, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_ready_now_cnt, -999) as STRING))
   OR trim(CAST(coalesce(stg.employee_ready_18_24_month_cnt, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_ready_18_24_month_cnt, -999) as STRING))
   OR trim(CAST(coalesce(stg.employee_ready_12_18_month_cnt, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_ready_12_18_month_cnt, -999) as STRING))
   OR trim(CAST(coalesce(stg.employee_ready_6_11_month_cnt, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_ready_6_11_month_cnt, -999) as STRING))
   OR trim(CAST(coalesce(stg.employee_other_readiness_cnt, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_other_readiness_cnt, -999) as STRING))
   OR trim(CAST(coalesce(stg.employee_readiness_unknown_cnt, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_readiness_unknown_cnt, -999) as STRING))
   OR trim(CAST(coalesce(stg.empl_slated_for_position_cnt, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_slated_for_position_cnt, -999) as STRING))
   OR trim(CAST(coalesce(stg.emp_talent_pooled_for_pos_cnt, -999) as STRING)) <> trim(CAST(coalesce(tgt.employee_talent_pooled_for_position_cnt, -999) as STRING))
   OR trim(CAST(coalesce(stg.lawson_company_num, -999) as STRING)) <> trim(CAST(coalesce(tgt.lawson_company_num, -999) as STRING))
   OR trim(coalesce(stg.process_level_code, '')) <> trim(coalesce(tgt.process_level_code, '')));

  INSERT INTO {{ params.param_hr_core_dataset_name }}.fact_employee_availability (employee_talent_profile_sid, valid_from_date, employee_sid, employee_num, valid_to_date, jobs_pooled_for_cnt, employee_talent_pool_cnt, employee_successor_cnt, employee_ready_now_cnt, employee_ready_18_24_month_cnt, employee_ready_12_18_month_cnt, employee_ready_6_11_month_cnt, employee_other_readiness_cnt, employee_readiness_unknown_cnt, employee_slated_for_position_cnt, employee_talent_pooled_for_position_cnt, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        stg.employee_talent_profile_sid,
        current_dt AS valid_from_date,
        stg.employee_sid,
        stg.employee_num,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.jobs_pooled_for_cnt,
        stg.employee_talent_pool_cnt,
        stg.employee_successor_cnt,
        stg.employee_ready_now_cnt,
        stg.employee_ready_18_24_month_cnt,
        stg.employee_ready_12_18_month_cnt,
        stg.employee_ready_6_11_month_cnt,
        stg.employee_other_readiness_cnt,
        stg.employee_readiness_unknown_cnt,
        stg.empl_slated_for_position_cnt,
        stg.emp_talent_pooled_for_pos_cnt,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.fact_employee_availability_wrk AS stg
      WHERE(coalesce(stg.employee_sid,0),  
             coalesce(stg.employee_num ,0),  
             coalesce(stg.employee_talent_profile_sid,0), 
             coalesce(stg.jobs_pooled_for_cnt,0), 
             coalesce(stg.employee_talent_pool_cnt,0),  
             coalesce(stg.employee_successor_cnt,0),  
             coalesce(stg.employee_ready_now_cnt,0),  
             coalesce(stg.employee_ready_18_24_month_cnt,0),  
             coalesce(stg.employee_ready_12_18_month_cnt,0),  
             coalesce(stg.employee_ready_6_11_month_cnt,0),  
             coalesce(stg.employee_other_readiness_cnt,0), 
             coalesce(stg.employee_readiness_unknown_cnt,0), 
             coalesce(stg.empl_slated_for_position_cnt,0),  
             coalesce(stg.emp_talent_pooled_for_pos_cnt,0),  
             coalesce(stg.lawson_company_num,0),  
             coalesce(trim(stg.process_level_code),''), 
             coalesce(trim(stg.source_system_code),'') )
        NOT IN(
        SELECT AS STRUCT
            coalesce(tgt.employee_sid,0),
            coalesce(tgt.employee_num,0), 
            coalesce(tgt.employee_talent_profile_sid,0),
            coalesce(tgt.jobs_pooled_for_cnt,0),
            coalesce(tgt.employee_talent_pool_cnt,0), 
            coalesce(tgt.employee_successor_cnt,0),
            coalesce(tgt.employee_ready_now_cnt,0), 
            coalesce(tgt.employee_ready_18_24_month_cnt,0), 
            coalesce(tgt.employee_ready_12_18_month_cnt,0), 
            coalesce(tgt.employee_ready_6_11_month_cnt,0),
            coalesce(tgt.employee_other_readiness_cnt,0), 
            coalesce(tgt.employee_readiness_unknown_cnt,0), 
            coalesce(tgt.employee_slated_for_position_cnt,0), 
            coalesce(tgt.employee_talent_pooled_for_position_cnt,0), 
            coalesce(tgt.lawson_company_num,0), 
            coalesce(trim(tgt.process_level_code),''), 
            coalesce(trim(tgt.source_system_code),'') 
          FROM
            {{ params.param_hr_base_views_dataset_name }}.fact_employee_availability AS tgt
          WHERE tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
      )
  ;

  UPDATE {{ params.param_hr_core_dataset_name }}.fact_employee_availability AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHERE tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
   AND tgt.employee_talent_profile_sid NOT IN(
    SELECT
        fact_employee_availability_wrk.employee_talent_profile_sid
      FROM
        {{ params.param_hr_stage_dataset_name }}.fact_employee_availability_wrk
      GROUP BY 1
  );


    /* Test Unique Index constraint set in Terdata */
    SET dup_count = ( 
        select count(*)
        from (
        select
            employee_talent_profile_sid ,valid_from_date
        from {{ params.param_hr_core_dataset_name }}.fact_employee_availability
        group by employee_talent_profile_sid ,valid_from_date
        having count(*) > 1
        )
    );
    IF dup_count <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.fact_employee_availability');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;
