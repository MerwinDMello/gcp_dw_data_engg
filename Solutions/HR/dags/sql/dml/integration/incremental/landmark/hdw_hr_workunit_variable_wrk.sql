BEGIN
DECLARE current_ts datetime;
SET current_ts = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.hr_workunit_variable_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hr_workunit_variable_wrk (workunit_sid, variable_seq_num, valid_from_date, valid_to_date, workunit_num, variable_name, variable_type_num, variable_value_text, lawson_company_num, process_level_code, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        b.workunit_sid,
        seqnbr AS variable_seq_num,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31T23:59:59") AS valid_to_date,
        pfiworkunit AS workunit_num,
        pfiworkunitvariable AS variable_name,
        variabletype AS variable_type_num,
        variablevalue AS variable_value_text,
        b.lawson_company_num,
        b.process_level_code,
        'Y' AS active_dw_ind,
        'L' AS source_system_code,
        timestamp_trunc(current_datetime('US/Central'),  SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.pfiworkunitvariable AS a
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit AS b ON a.pfiworkunit = b.workunit_num
         AND upper(b.source_system_code) = 'L'
         AND b.valid_to_date = datetime '9999-12-31 23:59:59'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.hr_workunit_variable_wrk (workunit_sid, variable_seq_num, valid_from_date, valid_to_date, workunit_num, variable_name, variable_type_num, variable_value_text, lawson_company_num, process_level_code, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        b.workunit_sid,
        seqnbr AS variable_seq_num,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31T23:59:59")  AS valid_to_date,
        pfiworkunit AS workunit_num,
        pfiworkunitvariable AS variable_name,
        variabletype AS variable_type_num,
        variablevalue AS variable_value_text,
        b.lawson_company_num,
        b.process_level_code,
        'Y' AS active_dw_ind,
        'B' AS source_system_code,
        timestamp_trunc(current_datetime('US/Central'),  SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_pfiworkunitvariable_stg AS a
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit AS b ON a.pfiworkunit = b.workunit_num
         AND upper(b.source_system_code) = 'B'
         AND b.valid_to_date = datetime '9999-12-31 23:59:59'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    UNION DISTINCT
    SELECT
        b.workunit_sid,
        5000 AS variable_seq_num,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31T23:59:59")  AS valid_to_date,
        pfiworkunit AS workunit_num,
        'Lawson_Company_Num' AS variable_name,
        3 AS variable_type_num,
        cast(org.hcaorgunitcompany as string) AS variable_value_text,
        b.lawson_company_num,
        b.process_level_code,
        'Y' AS active_dw_ind,
        'B' AS source_system_code,
        timestamp_trunc(current_datetime('US/Central'),  SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_pfiworkunitvariable_stg AS a
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit AS b ON a.pfiworkunit = b.workunit_num
         AND upper(b.source_system_code) = 'B'
         AND b.valid_to_date = datetime '9999-12-31 23:59:59'
        LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ats_cust_hrorganizationunit_stg AS org ON 
		cast(a.variablevalue as string) = cast(org.hrorganizationunit as string)
      WHERE upper(a.pfiworkunitvariable) = 'HRORGANIZATIONUNIT'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    UNION DISTINCT
    SELECT
        b.workunit_sid,
        5001 AS variable_seq_num,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31T23:59:59")  AS valid_to_date,
        pfiworkunit AS workunit_num,
        'Process_Level_Code' AS variable_name,
        3 AS variable_type_num,
        org.hcaorgunitprocesslevel AS variable_value_text,
        b.lawson_company_num,
        b.process_level_code,
        'Y' AS active_dw_ind,
        'B' AS source_system_code,
        timestamp_trunc(current_datetime('US/Central'),  SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_pfiworkunitvariable_stg AS a
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit AS b ON a.pfiworkunit = b.workunit_num
         AND upper(b.source_system_code) = 'B'
         AND b.valid_to_date = datetime '9999-12-31 23:59:59'
        LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ats_cust_hrorganizationunit_stg AS org ON 
		cast(a.variablevalue as string)= cast(org.hrorganizationunit as string)
      WHERE upper(a.pfiworkunitvariable) = 'HRORGANIZATIONUNIT'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ;
END;