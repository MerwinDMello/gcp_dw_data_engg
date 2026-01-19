BEGIN
DECLARE
DUP_COUNT INT64;
DECLARE
  current_ts datetime;
  set current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

call
  `{{ params.param_hr_core_dataset_name }}`.sk_gen(
    '{{ params.param_hr_stage_dataset_name }}',
    'deptcode',"Company||'-'||TRIM(Process_Level)||'-'||TRIM(Department)",'Department');

TRUNCATE TABLE   {{ params.param_hr_stage_dataset_name }}.department_wrk;

/*	Truncate Wrk1 Table		*/
BEGIN TRANSACTION;
  
   INSERT INTO {{ params.param_hr_stage_dataset_name }}.department_wrk (dept_sid, eff_from_date, process_level_sid, dept_code, dept_name, lawson_company_num, process_level_code, active_dw_ind, eff_to_date, security_key_text, source_system_code, dw_last_update_date_time)
    SELECT
        cast(st.dept_sid as INT64),
        st.eff_from_date,
        st.process_level_sid,
        st.dept_code,
        st.dept_name,
        st.lawson_company_num,
        st.process_level_code,
        st.active_dw_ind,
        date(st.eff_to_date),
        st.security_key_text,
        st.source_system_code,
        st.dw_last_update_date_time
      FROM
        (
          SELECT
              xwlk.sk AS dept_sid,
              /*For First load populate a lower value of Eff_From_Date*/
              -- ,CASE WHEN TGT.Dept_SID IS NULL THEN DEPT.DATE_STAMP ELSE CURRENT_DATE END AS Eff_From_Date
              current_date('US/Central') AS eff_from_date,
              coalesce(epi.process_level_sid, 0) AS process_level_sid,
              dept.department AS dept_code,
              dept.name AS dept_name,
              dept.company AS lawson_company_num,
              -- ,TRIM(DEPT.Process_Level) AS Process_Level_Code
              CASE
                WHEN cast(coalesce(dept.process_level, '') as string)  = '' THEN '00000'
                ELSE dept.process_level
              END AS process_level_code,
              'Y' AS active_dw_ind,
              '9999-12-31' AS eff_to_date,
              -- ,TRIM(DEPT.Company)||'-'||TRIM(DEPT.Process_Level)||'-'||TRIM(DEPT.Department) AS Security_Key_Text
              concat(substr('00000', 1, 5 - length(cast(dept.company as string))), cast(dept.company as string), '-', substr('00000', 1, 5 - length(cast(dept.process_level as string))), cast(dept.process_level as string), '-', substr('00000', 1, 5 - length(cast(dept.department as string))), cast(dept.department as string)) AS security_key_text,
              'L' AS source_system_code,
              current_ts AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.deptcode AS dept
              INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON 
			  
			 upper( substr(concat(dept.company, '-', cast(dept.process_level as string), '-', cast(dept.department as string)), 1, 255)) = upper(xwlk.sk_source_txt)
               AND upper(xwlk.sk_type) = 'DEPARTMENT'
              LEFT OUTER JOIN -- INNER JOIN $NCR_STG_SCHEMA.Ref_SK_Xwlk LKP_PRLVL_SID
                            (
                SELECT
                    process_level.process_level_sid,
                    process_level.lawson_company_num,
                    process_level.process_level_code
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.process_level
                  WHERE process_level.valid_to_date = datetime("9999-12-31 23:59:59") 
                   AND upper(process_level.source_system_code) = 'L'
                  GROUP BY 1, 2, 3
              ) AS epi ON dept.company = epi.lawson_company_num
               AND dept.process_level = epi.process_level_code
              LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.department AS tgt ON xwlk.sk = tgt.dept_sid
               AND upper(xwlk.sk_type) = 'DEPARTMENT'
               AND upper(tgt.active_dw_ind) = 'Y'
               AND upper(tgt.source_system_code) = 'L'
          ) AS st
  ;

  UPDATE {{ params.param_hr_core_dataset_name }}.department AS tgt SET 
  valid_to_date = current_ts - INTERVAL 1 SECOND, active_dw_ind = 'N', dw_last_update_date_time = current_ts FROM {{ params.param_hr_stage_dataset_name }}.department_wrk AS stg WHERE tgt.dept_sid = stg.dept_sid
   AND tgt.source_system_code = stg.source_system_code
   AND (tgt.process_level_sid <> stg.process_level_sid
   OR tgt.dept_code <> stg.dept_code
   OR upper(trim(tgt.dept_name)) <> upper(trim(stg.dept_name))
   OR tgt.lawson_company_num <> stg.lawson_company_num
   OR tgt.process_level_code <> stg.process_level_code)
   AND upper(tgt.active_dw_ind) = 'Y'
   AND tgt.valid_to_date = datetime("9999-12-31 23:59:59") 
   and tgt.source_system_code = 'L';
/* Retire records when records are no longer exists in staging table (Deptcode) on Department,process level and company*/

  UPDATE {{ params.param_hr_core_dataset_name }}.department AS tgt SET 
  valid_to_date = current_ts - INTERVAL 1 SECOND, active_dw_ind = 'N', dw_last_update_date_time = current_ts WHERE upper(tgt.active_dw_ind) = 'Y'
   AND (tgt.dept_code||tgt.process_level_code|| tgt.lawson_company_num) NOT IN(
    SELECT 
        stg.department||
        stg.process_level||
        stg.company
      FROM
        {{ params.param_hr_stage_dataset_name }}.deptcode AS stg
   
  )
   AND tgt.valid_to_date = datetime("9999-12-31 23:59:59") 
   and tgt.source_system_code = 'L';

  INSERT INTO {{ params.param_hr_core_dataset_name }}.department (dept_sid, valid_from_date, process_level_sid, dept_code, dept_name, lawson_company_num, process_level_code, active_dw_ind, valid_to_date, security_key_text, source_system_code, dw_last_update_date_time)
    SELECT
        department_wrk.dept_sid,
        --department_wrk.eff_from_date,
        current_ts,
        department_wrk.process_level_sid,
        department_wrk.dept_code,
        department_wrk.dept_name,
        department_wrk.lawson_company_num,
        department_wrk.process_level_code,
        department_wrk.active_dw_ind,
		datetime("9999-12-31 23:59:59"),
        --department_wrk.eff_to_date,
        department_wrk.security_key_text,
        department_wrk.source_system_code,
        department_wrk.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.department_wrk
      WHERE (
	 department_wrk.dept_sid 
	  ||department_wrk.process_level_sid
	  ||department_wrk.dept_code
	  ||upper(trim(department_wrk.dept_name))
	  ||department_wrk.lawson_company_num)
	  NOT IN(
        SELECT 
            department.dept_sid||
            department.process_level_sid||
            department.dept_code||
            upper(trim(department.dept_name))||
            department.lawson_company_num
          FROM
            {{ params.param_hr_core_dataset_name }}.department
          WHERE upper(department.active_dw_ind) = 'Y'
           AND department.valid_to_date = datetime("9999-12-31 23:59:59")
    
    );
    SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Dept_SID ,Valid_From_Date 
    FROM
      {{ params.param_hr_core_dataset_name }}.department
    GROUP BY
      Dept_SID ,Valid_From_Date 
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.hr_company');
  ELSE
COMMIT TRANSACTION;
END IF;
    
    END;
