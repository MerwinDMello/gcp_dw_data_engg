BEGIN
/**Unique primary index {{ params.param_hr_stage_dataset_name }}.APPLICANT_WRK1 and  {{ params.param_hr_core_dataset_name }}.Applicant
   /***Inserting into {{ params.param_hr_core_dataset_name }} staging applicant_wrk **/
  DECLARE DUP_COUNT INT64;
  DECLARE DUP_COUNT1 INT64;
  DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.applicant_wrk;
  
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.applicant_wrk (valid_from_date, valid_to_date, applicant_num, lawson_company_num, process_level_code, employee_num, source_system_code)
    SELECT DISTINCT
        current_ts AS valid_from_date,
       datetime("9999-12-31 23:59:59") AS valid_to_date,
        persaction.employee AS applicant_num,
        persaction.company AS lawson_company_num,
        CASE
          WHEN persaction.process_level IS NULL
           OR trim(persaction.process_level) = '' THEN '00000'
          ELSE persaction.process_level
        END AS process_level_code,
        persaction.participnt AS employee_num,
        'L' AS source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.persaction
      WHERE upper(persaction.action_type) = 'A'
    UNION DISTINCT
    SELECT DISTINCT
       current_ts AS valid_from_date,
         datetime("9999-12-31 23:59:59") AS valid_to_date,
        persacthst.employee AS applicant_num,
        persacthst.company AS lawson_company_num,
        '00000' AS process_level_code,
        0 AS employee_num,
        'L' AS source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.persacthst
      WHERE upper(persacthst.action_type) = 'A' ;
	  


/*  Generate the surrogate keys for Applicant_Load */
CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen("{{ params.param_hr_stage_dataset_name }}", 'applicant_wrk', "lawson_company_num||'-'||applicant_num", 'APPLICANT');

/***Truncating staging applicant_wrk1  **/

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.applicant_wrk1;
  
 BEGIN TRANSACTION;

/***Insert into staging applicant_wrk1 from staging applicant_wrk **/

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.applicant_wrk1 (applicant_sid, valid_from_date, valid_to_date, applicant_num, lawson_company_num, process_level_code, employee_num, source_system_code, dw_last_update_date_time)
    SELECT
        z.*
      FROM
        (
          SELECT
              cast(lkp_aplt_sid.sk as INT64) AS applicant_sid,
              current_ts AS valid_from_date,
               datetime("9999-12-31 23:59:59") AS valid_to_date,
              wrk.applicant_num,
              cast(wrk.lawson_company_num as INT64),
              wrk.process_level_code,
              wrk.employee_num,
              wrk.source_system_code,
              current_ts AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.applicant_wrk AS wrk
              INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS lkp_aplt_sid 
			  ON substr(wrk.lawson_company_num|| '-'||wrk.applicant_num, 1, 255) = lkp_aplt_sid.sk_source_txt
               AND upper(lkp_aplt_sid.sk_type) = 'APPLICANT'
        ) AS z
      QUALIFY row_number() OVER (PARTITION BY z.applicant_sid, z.valid_from_date ORDER BY z.applicant_sid, z.valid_from_date, z.employee_num DESC) = 1
  ;
SET DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT Applicant_SID ,Valid_From_Date
    FROM
    {{ params.param_hr_stage_dataset_name }}.applicant_wrk1
    GROUP BY Applicant_SID ,Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;
RAISE USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_stage_dataset_name }}.applicant_wrk1');
ELSE
COMMIT TRANSACTION;
END IF ;


BEGIN TRANSACTION ;
  /***{{ params.param_hr_core_dataset_name }}.applicant Update  **/
  UPDATE {{ params.param_hr_core_dataset_name }}.applicant AS tgt SET 
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = current_ts FROM {{ params.param_hr_stage_dataset_name }}.applicant_wrk1 AS stg WHERE tgt.applicant_sid = stg.applicant_sid
   AND tgt.source_system_code = stg.source_system_code
   AND (trim(tgt.process_level_code) <> trim(stg.process_level_code)
   OR tgt.employee_num <> stg.employee_num)
   AND date(tgt.valid_to_date) = "9999-12-31" 
    AND tgt.source_system_code = 'L';
   
 /***{{ params.param_hr_core_dataset_name }}.applicant Insert **/
  INSERT INTO {{ params.param_hr_core_dataset_name }}.applicant (applicant_sid, valid_from_date, valid_to_date, applicant_num, lawson_company_num, process_level_code, employee_num, source_system_code, dw_last_update_date_time)
    SELECT
        stg.applicant_sid,
        current_ts AS valid_from_date,
        datetime("9999-12-31 23:59:59") AS valid_to_date,
        stg.applicant_num,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.employee_num,
        stg.source_system_code,
        current_ts
      FROM
        {{ params.param_hr_stage_dataset_name }}.applicant_wrk1 AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.applicant AS tgt ON stg.applicant_sid = tgt.applicant_sid
         AND tgt.valid_to_date = datetime("9999-12-31 23:59:59")
      WHERE tgt.applicant_sid IS NULL
  ;
 

 
  /***{{ params.param_hr_core_dataset_name }}.applicant update from applicant_wrk1 staging **/

  UPDATE {{ params.param_hr_core_dataset_name }}.applicant AS tgt 
  
  SET valid_to_date = current_ts - INTERVAL 1 SECOND, 
  
  dw_last_update_date_time = current_ts WHERE tgt.applicant_sid NOT IN(
    SELECT
        applicant_wrk1.applicant_sid
      FROM
        {{ params.param_hr_stage_dataset_name }}.applicant_wrk1
      GROUP BY 1
  )
   AND tgt.valid_to_date = datetime("9999-12-31 23:59:59")
  AND tgt.source_system_code = 'L';
  
SET DUP_COUNT1 = (
  SELECT
    COUNT(*)
  FROM (
    SELECT Applicant_SID ,Valid_From_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.applicant
    GROUP BY Applicant_SID ,Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF DUP_COUNT1 <> 0 THEN
ROLLBACK TRANSACTION;
RAISE USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.applicant');
ELSE
COMMIT TRANSACTION;
END IF ;
END;
