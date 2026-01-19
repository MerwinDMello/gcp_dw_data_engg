/*  Generate the surrogate keys */

  CALL `{{ params.param_hr_core_dataset_name }}.sk_gen`("{{ params.param_hr_stage_dataset_name }}","work_history_report","COALESCE(TRIM(work_history_id),'')","TMS_Employee_Work_History");

/*Insert Bad character records and the records that are left out after inner join with EDWHR.Employee_Talent_Profile into Reject*/
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_work_history_reject;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_work_history_reject 
    SELECT
        stg.employee_id,
        stg.work_history_company_name,
        stg.work_history_job_title,
        stg.work_history_description,
        stg.work_history_start_date,
        stg.work_history_end_date,
        stg.work_history_address,
        stg.work_history_city,
        stg.work_history_region,
        stg.work_history_country,
        stg.work_history_postal_code,
        stg.work_history_id,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
        CASE
          WHEN trim(stg.employee_id) IS NULL THEN 'Employee_ID is NULL'
          WHEN substr(trim(stg.employee_id), 1, 1) NOT IN(
            '1', '2', '3', '4', '5', '6', '7', '8', '9'
          ) THEN 'Employee_Id is alpha_numeric'
          ELSE CAST(0 as STRING)
        END AS reject_reason,
        'Employee_Work_History' AS reject_stg_tbl_nm
      FROM
        {{ params.param_hr_stage_dataset_name }}.work_history_report AS stg
      WHERE substr(trim(stg.employee_id), 1, 1) NOT IN(
        '1', '2', '3', '4', '5', '6', '7', '8', '9'
      )
       OR substr(trim(stg.employee_id), 1, 1) IN(
        '1', '2', '3', '4', '5', '6', '7', '8', '9'
      )
       AND CASE
         trim(stg.employee_id)
        WHEN '' THEN 0
        ELSE SAFE_CAST(trim(stg.employee_id) as INT64)
      END NOT IN(
        SELECT
            employee_num AS employee_num
          FROM
            {{ params.param_hr_base_views_dataset_name }}.employee
      )
  ;
  


BEGIN
  DECLARE dup_count INT64;

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_work_history_wrk;


  BEGIN TRANSACTION;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_work_history_wrk (employee_work_history_sid, valid_from_date, employee_num, employee_sid, employee_talent_profile_sid, previous_work_address_sid, work_history_company_name, work_history_job_title_text, work_history_desc, work_history_start_date, work_history_end_date, lawson_company_num, process_level_code, valid_to_date, source_system_key, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(xwlk.sk AS INT64) AS employee_work_history_sid,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS valid_from_date,
        SAFE_CAST(whr.employee_id AS INT64) AS employee_num,
        emp.employee_sid,
        etp.employee_talent_profile_sid,
        addr.addr_sid AS previous_work_address_sid,
        whr.work_history_company_name,
        whr.work_history_job_title,
        whr.work_history_description,
        CAST(CONCAT(REGEXP_EXTRACT(whr.work_history_start_date, r'\/([0-9]{4})'), "-", REGEXP_EXTRACT(whr.work_history_start_date, r'([0-9]{1,2})\/'), "-", REGEXP_EXTRACT(whr.work_history_start_date, r'\/([0-9]{1,2})\/')) AS DATE) AS work_history_start_date,
        CAST(CONCAT(REGEXP_EXTRACT(whr.work_history_end_date, r'\/([0-9]{4})'), "-", REGEXP_EXTRACT(whr.work_history_end_date, r'([0-9]{1,2})\/'), "-", REGEXP_EXTRACT(whr.work_history_end_date, r'\/([0-9]{1,2})\/')) AS DATE) AS work_history_end_date,
        etp.lawson_company_num,
        max(coalesce(etp.process_level_code, '00000')) AS process_level_code,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        whr.work_history_id AS source_system_key,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.work_history_report AS whr
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk 
        ON trim(whr.work_history_id) = trim(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'TMS_EMPLOYEE_WORK_HISTORY'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp 
        ON CASE
           trim(whr.employee_id)
          WHEN '' THEN 0
          ELSE SAFE_CAST(trim(whr.employee_id) as INT64)
        END = emp.employee_num
         AND emp.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND SAFE_CAST(coalesce(whr.employee_id, '0') AS NUMERIC) IS NOT NULL 
         AND emp.lawson_company_num = CASE
           substr(whr.job_code, 1, 4)
          WHEN '' THEN 0
          ELSE CAST(substr(whr.job_code, 1, 4) as INT64)
        END
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_talent_profile AS etp ON CASE
           trim(whr.employee_id)
          WHEN '' THEN 0
          ELSE SAFE_CAST(trim(whr.employee_id) as INT64)
        END = etp.employee_num
         AND etp.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND SAFE_CAST(coalesce(whr.employee_id, '0') AS NUMERIC) IS NOT NULL 
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.address AS addr ON coalesce(trim(whr.work_history_address), '') = coalesce(addr.addr_line_1_text, '')
         AND coalesce(trim(whr.work_history_city), '') = coalesce(trim(addr.city_name), '')
         AND coalesce(trim(whr.work_history_country), '') = coalesce(trim(addr.country_code), '')
         AND coalesce(trim(whr.work_history_postal_code), '') = coalesce(trim(addr.zip_code), '')
         AND trim(addr.addr_type_code) = 'PWR'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, upper(coalesce(etp.process_level_code, '00000')), 14, 15
    ;


    /* Test Unique Index constraint set in Terdata */
    SET dup_count = ( 
        select count(*)
        from (
        select
            employee_work_history_sid ,valid_from_date
        from {{ params.param_hr_stage_dataset_name }}.employee_work_history_wrk
        group by employee_work_history_sid ,valid_from_date
        having count(*) > 1
        )
    );
    IF dup_count <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_stage_dataset_name }}.employee_work_history_wrk');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;
