BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
BEGIN TRANSACTION;

/*  De-Activate the Base Table records      */
  UPDATE {{ params.param_hr_core_dataset_name }}.job_personnel_code AS tgt SET valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = current_ts FROM {{ params.param_hr_stage_dataset_name }}.job_personnel_code_wrk AS stg WHERE stg.job_code_sid = tgt.job_code_sid
   AND stg.position_sid = tgt.position_sid
   AND stg.personnel_type_code = tgt.personnel_type_code
   AND stg.personnel_code = tgt.personnel_code
   AND stg.hr_company_sid = tgt.hr_company_sid
   AND (upper(trim(coalesce(tgt.required_flag_ind, ''))) <> upper(trim(coalesce(stg.required_flag_ind, '')))
   OR upper(trim(coalesce(cast(tgt.personnel_code_time_pct as string), ''))) <> upper(trim(coalesce(cast(stg.personnel_code_time_pct as string), '')))
   OR upper(trim(coalesce(tgt.proficiency_level_desc, ''))) <> upper(trim(coalesce(stg.proficiency_level_desc, '')))
   OR upper(trim(coalesce(cast(tgt.weight_amt as string), ''))) <> upper(trim(coalesce(cast(stg.weight_amt as string), '')))
   OR upper(trim(coalesce(tgt.subject_code, ''))) <> upper(trim(coalesce(stg.subject_code, '')))
   OR upper(trim(coalesce(tgt.job_code, ''))) <> upper(trim(coalesce(stg.job_code, '')))
   OR upper(trim(coalesce(tgt.position_code, ''))) <> upper(trim(coalesce(stg.position_code, '')))
   OR upper(trim(coalesce(cast(tgt.lawson_company_num as string), ''))) <> upper(trim(coalesce(cast(stg.lawson_company_num as string), '')))
   OR upper(trim(coalesce(tgt.process_level_code, ''))) <> upper(trim(coalesce(stg.process_level_code, '')))
   OR upper(trim(coalesce(tgt.source_system_code, ''))) <> upper(trim(coalesce(stg.source_system_code, ''))))
   AND tgt.valid_to_date =  DATETIME("9999-12-31 23:59:59")
   AND tgt.source_system_code = 'L';

/*  Insert the New Records into the Base Table  */
  INSERT INTO {{ params.param_hr_core_dataset_name }}.job_personnel_code (job_code_sid, position_sid, personnel_type_code, personnel_code, hr_company_sid, valid_from_date, valid_to_date, required_flag_ind, personnel_code_time_pct, proficiency_level_desc, weight_amt, subject_code, job_code, position_code, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        stg.job_code_sid,
        stg.position_sid,
        stg.personnel_type_code,
        stg.personnel_code,
        stg.hr_company_sid,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.required_flag_ind,
        stg.personnel_code_time_pct,
        stg.proficiency_level_desc,
        stg.weight_amt,
        stg.subject_code,
        stg.job_code,
        stg.position_code,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.source_system_code,
        current_ts
      FROM
        {{ params.param_hr_stage_dataset_name }}.job_personnel_code_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.job_personnel_code AS tgt ON stg.job_code_sid = tgt.job_code_sid
         AND stg.position_sid = tgt.position_sid
         AND stg.personnel_type_code = tgt.personnel_type_code
         AND stg.personnel_code = tgt.personnel_code
         AND stg.hr_company_sid = tgt.hr_company_sid
         AND upper(trim(coalesce(tgt.required_flag_ind, ''))) = upper(trim(coalesce(stg.required_flag_ind, '')))
         AND upper(trim(coalesce(cast(tgt.personnel_code_time_pct as string), ''))) = upper(trim(coalesce(cast(stg.personnel_code_time_pct as string), '')))
         AND upper(trim(coalesce(tgt.proficiency_level_desc, ''))) = upper(trim(coalesce(stg.proficiency_level_desc, '')))
         AND upper(trim(coalesce(cast(tgt.weight_amt as string), ''))) = upper(trim(coalesce(cast(stg.weight_amt as string), '')))
         AND upper(trim(coalesce(tgt.subject_code, ''))) = upper(trim(coalesce(stg.subject_code, '')))
         AND upper(trim(coalesce(tgt.job_code, ''))) = upper(trim(coalesce(stg.job_code, '')))
         AND upper(trim(coalesce(tgt.position_code, ''))) = upper(trim(coalesce(stg.position_code, '')))
         AND upper(trim(coalesce(cast(tgt.lawson_company_num as string), ''))) = upper(trim(coalesce(cast(stg.lawson_company_num as string), '')))
         AND upper(trim(coalesce(tgt.process_level_code, ''))) = upper(trim(coalesce(stg.process_level_code, '')))
         AND upper(trim(coalesce(tgt.source_system_code, ''))) = upper(trim(coalesce(stg.source_system_code, '')))
         AND tgt.valid_to_date =  DATETIME("9999-12-31 23:59:59")
      WHERE tgt.job_code_sid IS NULL
  ;
  
  /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Job_Code_SID ,Position_SID ,Personnel_Type_Code ,Personnel_Code ,HR_Company_SID ,Valid_From_Date
        from {{ params.param_hr_core_dataset_name }}.job_personnel_code
        group by Job_Code_SID ,Position_SID ,Personnel_Type_Code ,Personnel_Code ,HR_Company_SID ,Valid_From_Date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.job_personnel_code');
    ELSE
      COMMIT TRANSACTION;
    END IF;

/*  RETIRE RECORD ON 2ND RETIRE LOGIC */
  UPDATE {{ params.param_hr_core_dataset_name }}.job_personnel_code AS tgt SET valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = current_ts WHERE tgt.valid_to_date =  DATETIME("9999-12-31 23:59:59")
   AND (tgt.job_code_sid, tgt.position_sid, tgt.personnel_type_code, tgt.personnel_code, tgt.hr_company_sid) NOT IN(
    SELECT AS STRUCT
        job_personnel_code_wrk.job_code_sid,
        job_personnel_code_wrk.position_sid,
        job_personnel_code_wrk.personnel_type_code,
        job_personnel_code_wrk.personnel_code,
        job_personnel_code_wrk.hr_company_sid
      FROM
        {{ params.param_hr_stage_dataset_name }}.job_personnel_code_wrk
      GROUP BY 1, 2, 3, 4, 5
  ) 
  AND tgt.source_system_code = 'L';

END;