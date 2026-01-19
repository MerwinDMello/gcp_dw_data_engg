BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.gl_lawson_dept_crosswalk_wrk;

BEGIN TRANSACTION;

/* Load Work Table with working Data */
INSERT INTO {{ params.param_hr_stage_dataset_name }}.gl_lawson_dept_crosswalk_wrk (gl_company_num, account_unit_num, valid_from_date, valid_to_date, coid, unit_num, dept_num, process_level_code, lawson_company_num, security_key_text, company_code, source_system_code, dw_last_update_date_time)
    SELECT
        stg.company AS gl_company_num,
        stg.acct_unit AS account_unit_num,
        current_ts AS valid_from_date,
        datetime("9999-12-31 23:59:59") AS valid_to_date,
        stg.hca_coid AS coid,
        stg.hca_unit AS unit_num,
        cast(stg.hca_dept as string) AS dept_num,
        CASE
          WHEN trim(stg.process_level) IS NULL
           OR trim(stg.process_level) = '' THEN '00000'
          ELSE concat(substr('00000', 1, 5 - length(trim(stg.process_level))), trim(stg.process_level))
        END AS process_level_code,
        stg.hr_company AS lawson_company_num,
        -- ,CASE WHEN TRIM(HR_COMPANY)='' THEN '00000' ELSE HR_COMPANY END||'-'||CASE WHEN TRIM(PROCESS_LEVEL)='' THEN '00000' ELSE PROCESS_LEVEL END||'-'||CASE WHEN TRIM(CAST(ACCT_UNIT AS CHAR(5)))='' THEN '00000' ELSE ACCT_UNIT END AS SECURITY_KEY_TEXT
        concat(CASE
          WHEN stg.hr_company IS NULL
           OR trim(cast (stg.hr_company as string)) = '' THEN '00000'
          ELSE concat(substr('00000', 1, 5 - length(trim(cast(stg.hr_company as string)))), trim(cast(stg.hr_company as string)))
        END, '-', CASE
          WHEN trim(stg.process_level) IS NULL
           OR trim(stg.process_level) = '' THEN '00000'
          ELSE concat(substr('00000', 1, 5 - length(trim(stg.process_level))), trim(stg.process_level))
        END, '-', CASE
          WHEN trim(stg.acct_unit) IS NULL
           OR trim(stg.acct_unit) = '' THEN '00000'
          ELSE concat(substr('00000', 1, 5 - length(trim(stg.acct_unit))), trim(stg.acct_unit))
        END) AS security_key_text,
        'H' AS company_code,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.zxauxref AS stg;
   
   /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Coid ,Dept_Num ,Process_Level_Code ,Security_Key_Text
        from {{ params.param_hr_stage_dataset_name }}.gl_lawson_dept_crosswalk_wrk
        group by Coid ,Dept_Num ,Process_Level_Code ,Security_Key_Text
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: gl_lawson_dept_crpsswalk_wrk');
    ELSE
      COMMIT TRANSACTION;
    END IF;
   
   END;
