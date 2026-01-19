CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_user', 'CAST(USERNO as STRING)', 'RECRUITMENT_USER');
CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'ats_cust_v2_recruiter_stg', 'CAST(Recruiter as STRING)||\'-ATS\'', 'RECRUITMENT_USER');
CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'ats_cust_jobrequisition_stg', 'CAST(HiringManager as STRING)||\'-ATS\'', 'RECRUITMENT_USER');
CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'ats_cust_jobrequisition_stg', 'CAST(Creator as STRING)||\'-ATS\'', 'RECRUITMENT_USER');


/*  Truncate Worktable Table     */
BEGIN
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.taleo_user_wrk;
/*  Load Work Table with working Data */
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.taleo_user_wrk (file_date, recruitment_user_sid, valid_from_date, recruitment_user_num, valid_to_date, employee_num, employee_34_login_code, first_name, last_name, source_system_code, dw_last_update_date_time)
    SELECT
        stg.file_date AS file_date,
        cast(xwlk.sk as int64) AS recruitment_user_sid,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
        CASE
          WHEN stg.userno IS NULL THEN 0
          ELSE stg.userno
        END AS recruitment_user_num,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        SAFE_CAST(trim(stg.employeeid) AS INT64) AS employee_num,
        substr(concat(trim(stg.useraccount_loginname), '       '), 1, 7) AS employee_34_login_code,
        substr(trim(stg.firstname), 1, 50) AS first_name,
        substr(trim(stg.lastname), 1, 50) AS last_name,
        source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_user AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON trim(cast(stg.userno as string)) = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'RECRUITMENT_USER'
    UNION ALL
    SELECT
        CURRENT_DATE('US/Central') AS file_date,
        cast(xwlk.sk as int64) AS recruitment_user_sid,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
        CAST(stg.employee as INT64) AS recruitment_user_num,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(stg.employee as INT64) AS employee_num,
        emp.legacyemploymentnumber AS employee_34_login_code,
        emp.name_givenname AS first_name,
        emp.name_familyname AS last_name,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              CAST(stg_0.hiringmanager as INT64) AS employee
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS stg_0
          UNION DISTINCT
          SELECT DISTINCT
              CAST(stg_0.recruiter as INT64)  AS employee
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_recruiter_stg AS stg_0
          UNION DISTINCT
          SELECT DISTINCT
              stg_0.creator AS employee
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS stg_0
        ) AS stg
        LEFT OUTER JOIN (
          SELECT
              ats_cust_employee_stg.employee,
              ats_cust_employee_stg.legacyemploymentnumber,
              ats_cust_employee_stg.name_givenname,
              ats_cust_employee_stg.name_familyname,
              coalesce(CAST(substr(trim(CAST(ats_cust_employee_stg.updatestamp as STRING)), 1, 19) as TIMESTAMP), TIMESTAMP '1901-01-01 00:00:00') AS last_modified_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_employee_stg
            QUALIFY row_number() OVER (PARTITION BY ats_cust_employee_stg.employee ORDER BY unix_seconds(last_modified_date_time) DESC) = 1
        ) AS emp ON stg.employee = CAST(emp.employee as INT64)
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(substr(trim(concat(stg.employee, '-ATS')), 1, 255)) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'RECRUITMENT_USER'
  ;
END;