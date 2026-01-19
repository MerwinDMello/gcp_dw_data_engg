BEGIN
  DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.job_personnel_code_wrk;

/*  Load Wrk Table with Today's records */
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.job_personnel_code_wrk (job_code_sid, position_sid, personnel_type_code, personnel_code, hr_company_sid, valid_from_date, valid_to_date, required_flag_ind, personnel_code_time_pct, proficiency_level_desc, weight_amt, subject_code, job_code, position_code, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        coalesce(jd.job_code_sid, 0) AS job_code_sid,
        coalesce(jp.position_sid, 0) AS position_sid,
        stg.type AS personnel_type_code,
        stg.pers_code AS personnel_code,
        coalesce(comp.hr_company_sid, 0) AS hr_company_sid,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.required_flag AS required_flag_ind,
        stg.pct_of_time AS personnel_code_time_pct,
        stg.profic_level AS proficiency_level_desc,
        stg.wght AS weight_amt,
        stg.subject_code AS subject_code,
        stg.job_code AS job_code,
        stg.position AS position_code,
        stg.company AS lawson_company_num,
        stg.process_level AS process_level_code,
        stg.source_system_code AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.jobrelate AS stg
        INNER JOIN -- INNER JOIN $NCR_STG_SCHEMA.REF_SK_XWLK XWLK
        -- ON (CAST(TRIM(STG.COMPANY)||'-'||TRIM(STG."POSITION") AS VARCHAR(255)) = XWLK.SK_SOURCE_TXT AND XWLK.SK_TYPE = 'POSITION')
        (
          SELECT
              job_position.position_sid,
              job_position.eff_from_date,
              job_position.position_code,
              job_position.lawson_company_num
            FROM
              {{ params.param_hr_core_dataset_name }}.job_position
            WHERE job_position.valid_to_date = DATETIME("9999-12-31 23:59:59") 
             AND upper(job_position.source_system_code) = 'L'
            QUALIFY row_number() OVER (PARTITION BY job_position.position_sid ORDER BY job_position.eff_from_date DESC) = 1
        ) AS jp ON stg.position = jp.position_code
         AND stg.company = jp.lawson_company_num
        LEFT OUTER JOIN (
          SELECT
              job_code.job_code_sid,
              job_code.eff_from_date,
              job_code.job_code,
              job_code.lawson_company_num
            FROM
              {{ params.param_hr_core_dataset_name }}.job_code
            WHERE job_code.valid_to_date = DATETIME("9999-12-31 23:59:59") 
             AND upper(job_code.source_system_code) = 'L'
            QUALIFY row_number() OVER (PARTITION BY job_code_sid ORDER BY eff_from_date DESC) = 1
        ) AS jd ON stg.company = jd.lawson_company_num
         AND stg.job_code = jd.job_code
        LEFT OUTER JOIN (
          SELECT
              hr_company.hr_company_sid,
              hr_company.valid_from_date,
              hr_company.lawson_company_num
            FROM
              {{ params.param_hr_core_dataset_name }}.hr_company
            WHERE hr_company.valid_to_date = DATETIME("9999-12-31 23:59:59") 
            QUALIFY row_number() OVER (PARTITION BY hr_company.hr_company_sid ORDER BY hr_company.valid_from_date DESC) = 1
        ) AS comp ON stg.company = comp.lawson_company_num;

END;