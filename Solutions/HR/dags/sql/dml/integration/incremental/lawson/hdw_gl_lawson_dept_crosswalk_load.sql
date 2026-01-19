BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.gl_lawson_dept_crosswalk AS tgt SET valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = stg.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.gl_lawson_dept_crosswalk_wrk AS stg WHERE tgt.gl_company_num = stg.gl_company_num
   AND trim(tgt.account_unit_num) = trim(stg.account_unit_num)
   AND trim(tgt.process_level_code) = trim(stg.process_level_code)
   AND (upper(trim(coalesce(tgt.coid, ''))) <> upper(trim(coalesce(stg.coid, '')))
   OR upper(trim(coalesce(tgt.unit_num, ''))) <> upper(trim(coalesce(stg.unit_num, '')))
   OR upper(trim(coalesce(tgt.dept_num, ''))) <> upper(trim(coalesce(stg.dept_num, '')))
   OR trim(CAST(coalesce(tgt.lawson_company_num, -999) as STRING)) <> trim(CAST(coalesce(stg.lawson_company_num, -999) as STRING)))
   AND tgt.valid_to_date =  datetime("9999-12-31 23:59:59")
   AND tgt.source_system_code = 'L';

  INSERT INTO {{ params.param_hr_core_dataset_name }}.gl_lawson_dept_crosswalk (gl_company_num, account_unit_num, valid_from_date, valid_to_date, coid, unit_num, dept_num, process_level_code, lawson_company_num, security_key_text, company_code, source_system_code, dw_last_update_date_time)
    SELECT
        stg.gl_company_num,
        stg.account_unit_num,
        current_ts,
        stg.valid_to_date,
        stg.coid,
        stg.unit_num,
        stg.dept_num,
        stg.process_level_code,
        stg.lawson_company_num,
        stg.security_key_text,
        stg.company_code,
        stg.source_system_code,
        current_ts
      FROM
        {{ params.param_hr_stage_dataset_name }}.gl_lawson_dept_crosswalk_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.gl_lawson_dept_crosswalk AS tgt ON stg.gl_company_num = tgt.gl_company_num
         AND trim(tgt.account_unit_num) = trim(stg.account_unit_num)
         AND upper(trim(coalesce(tgt.coid, ''))) = upper(trim(coalesce(stg.coid, '')))
         AND tgt.unit_num = stg.unit_num
         AND trim(tgt.dept_num) = trim(stg.dept_num)
         AND upper(trim(coalesce(tgt.process_level_code, ''))) = upper(trim(coalesce(stg.process_level_code, '')))
         AND trim(CAST(coalesce(tgt.lawson_company_num, 999) as STRING)) = trim(CAST(coalesce(stg.lawson_company_num, 999) as STRING))
         AND tgt.valid_to_date =  datetime("9999-12-31 23:59:59")
      WHERE tgt.gl_company_num IS NULL;
      
/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select GL_Company_Num ,Account_Unit_Num ,Process_Level_Code ,Valid_From_Date
        from {{ params.param_hr_core_dataset_name }}.gl_lawson_dept_crosswalk
        group by GL_Company_Num , Account_Unit_Num ,Process_Level_Code ,Valid_From_Date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.gl_lawson_dept_crosswalk');
    ELSE
      COMMIT TRANSACTION;
    END IF;  
  UPDATE {{ params.param_hr_core_dataset_name }}.gl_lawson_dept_crosswalk AS tgt SET valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = current_ts WHERE tgt.valid_to_date = datetime("9999-12-31 23:59:59")
   AND (tgt.gl_company_num, tgt.account_unit_num, tgt.process_level_code) NOT IN(
    SELECT AS STRUCT
        stg.gl_company_num,
        stg.account_unit_num,
        stg.process_level_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.gl_lawson_dept_crosswalk_wrk AS stg
  )AND tgt.source_system_code = 'L';
  
END;