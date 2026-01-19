BEGIN
DECLARE
  DUP_COUNT INT64;
  DECLARE
    current_ts datetime;
  set current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.requisition_process_level_wrk;

 call
  `{{ params.param_hr_core_dataset_name }}`.sk_gen("{{ params.param_hr_stage_dataset_name }}",
    'pajobreq', "cast(company as string) ||'-'||cast(Process_Level  as string)", 'Process_Level');


/*  Load Work Table with working Data for First load*/
BEGIN TRANSACTION;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.requisition_process_level_wrk (requisition_sid, process_level_sid, lawson_company_num, process_level_code, requisition_num, security_key_text, valid_from_date, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        -- ,Dept_Code
        req.requisition_sid,
       cast( xwlk.sk as int64) AS process_level_sid,
        pajr. company,
        -- ,PAJR.Process_Level
        CASE
          WHEN upper(trim(coalesce(pajr.process_level, ''))) = '' THEN '00000'
          ELSE trim(pajr.process_level)
        END AS process_level_code,
        pajr.requisition,
        concat(substr('00000', 1, 5 - length(trim(cast(pajr.company as string)))), trim(cast(pajr.company as string)), '-', CASE
          WHEN trim(pajr.process_level) IS NULL
           OR trim(pajr.process_level) = '' THEN '00000'
          ELSE concat(substr('00000', 1, 5 - length(trim(pajr.process_level))), trim(pajr.process_level))
        END, '-', '00000') AS security_key_text,
        current_ts AS valid_from_date ,
        datetime("9999-12-31 23:59:59") AS valid_to_date,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.pajobreq AS pajr
        INNER JOIN {{ params.param_hr_core_dataset_name }}.requisition AS req ON pajr.requisition = req.requisition_num
         AND pajr.company = req.lawson_company_num
         AND upper(req.active_dw_ind) = 'Y'
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(substr(concat(trim(coalesce(cast(pajr.company as string), '')), '-', CASE
          WHEN upper(trim(coalesce(pajr.process_level, ''))) = '' THEN '00000'
          ELSE trim(pajr.process_level)
        END), 1, 255)) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'PROCESS_LEVEL'
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.requisition_process_level AS tgt ON trim(cast(req.requisition_sid as string)) = trim(cast(tgt.requisition_sid as string))
         AND trim(cast(xwlk.sk as string)) = trim(cast(tgt.process_level_sid as string))
         AND tgt.valid_to_date = datetime("9999-12-31 23:59:59")
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  ;

/*UPDATE VLD_TO_DATE from records that are not presnt in Staging table*/

  UPDATE {{ params.param_hr_core_dataset_name }}.requisition_process_level AS tgt SET valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = current_ts WHERE tgt.valid_to_date = datetime("9999-12-31 23:59:59")
   AND trim(cast(tgt.requisition_sid as string)) NOT IN(
    SELECT
        trim(cast(requisition_process_level_wrk.requisition_sid as string))
      FROM
        {{ params.param_hr_stage_dataset_name }}.requisition_process_level_wrk
  ) AND tgt.source_system_code = 'L';

  UPDATE {{ params.param_hr_core_dataset_name }}.requisition_process_level AS jclass SET valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = current_ts FROM (
    SELECT
        requisition_process_level_wrk.requisition_sid,
        requisition_process_level_wrk.process_level_sid,
        requisition_process_level_wrk.lawson_company_num,
        requisition_process_level_wrk.process_level_code,
        -- Dept_Code,
        requisition_process_level_wrk.requisition_num,
        requisition_process_level_wrk.valid_from_date
      FROM
        {{ params.param_hr_stage_dataset_name }}.requisition_process_level_wrk
  ) AS stg WHERE jclass.requisition_sid = stg.requisition_sid
   AND (trim(cast(jclass.process_level_sid as string)) <> trim(cast(stg.process_level_sid as string))
   OR trim(cast(jclass.lawson_company_num as string)) <> trim(cast(stg.lawson_company_num as string))
   OR trim(cast(jclass.process_level_code as string)) <> trim(cast(stg.process_level_code as string))
   OR trim(cast(jclass.requisition_num as string)) <> trim(cast(stg.requisition_num as string)))
   AND (jclass.valid_to_date) = datetime("9999-12-31 23:59:59")
   and jclass.source_system_code = 'L';

--     TRIM(JCLASS.Process_Level_SID) <> TRIM(STG.Process_Level_SID)
-- OR   TRIM(JCLASS.Dept_Code) NOT= TRIM(STG.Dept_Code)

  INSERT INTO {{ params.param_hr_core_dataset_name }}.requisition_process_level (requisition_sid, process_level_sid, lawson_company_num, process_level_code, requisition_num, security_key_text, valid_from_date, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        -- ,Dept_Code
        requisition_process_level_wrk.requisition_sid,
        requisition_process_level_wrk.process_level_sid,
        requisition_process_level_wrk.lawson_company_num,
        requisition_process_level_wrk.process_level_code,
        -- ,Dept_Code
        requisition_process_level_wrk.requisition_num,
        requisition_process_level_wrk.security_key_text,
        requisition_process_level_wrk.valid_from_date,
        requisition_process_level_wrk.valid_to_date,
        requisition_process_level_wrk.source_system_code,
        requisition_process_level_wrk.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.requisition_process_level_wrk
      WHERE (trim(cast(requisition_process_level_wrk.requisition_sid as string)),
      trim(cast(requisition_process_level_wrk.process_level_sid as string)), 
      trim(cast(requisition_process_level_wrk.lawson_company_num as string)),
      trim(cast(requisition_process_level_wrk.process_level_code as string)), 
      trim(cast(requisition_process_level_wrk.requisition_num as string))) NOT IN(
        SELECT AS STRUCT
            -- TRIM(Dept_Code),
            trim(cast(requisition_process_level.requisition_sid as string)),
            trim(cast(requisition_process_level.process_level_sid as string)),
            trim(cast(requisition_process_level.lawson_company_num as string)),
            trim(cast(requisition_process_level.process_level_code as string)),
            -- ,TRIM(Dept_Code)
            trim(cast(requisition_process_level.requisition_num as string))
          FROM
            {{ params.param_hr_core_dataset_name }}.requisition_process_level
          WHERE requisition_process_level.valid_to_date = datetime("9999-12-31 23:59:59")
          and source_system_code = 'L'
      )
  ;

SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Requisition_SID ,Valid_From_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.requisition_process_level
    GROUP BY
      Requisition_SID ,Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.requisition_process_level');
  ELSE
COMMIT TRANSACTION;
END IF;
END
