  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.education_history_report_reject;
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.education_history_report_reject (employee_id, school_name, school_type, degree, major, education_start_date, education_end_date, year_graduated, gpa, education_comments, edu_hist_record_id, dw_last_update_date_time, reject_reason, reject_stg_tbl_nm)
    SELECT
        stg.employee_id,
        stg.school_name,
        stg.school_type,
        stg.degree,
        stg.major,
        stg.education_start_date,
        stg.education_end_date,
        stg.year_graduated,
        stg.gpa,
        stg.education_comments,
        stg.edu_hist_record_id,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
        CASE
          WHEN trim(stg.employee_id) IS NULL THEN 'Employee_ID is NULL'
          WHEN substr(trim(stg.employee_id), 1, 1) NOT IN(
            '1', '2', '3', '4', '5', '6', '7', '8', '9'
          ) THEN 'Employee_Id is alpha_numeric'
          ELSE CAST(0 as STRING)
        END AS reject_reason,
        'Employee' AS reject_stg_tbl_nm
      FROM
        {{ params.param_hr_stage_dataset_name }}.education_history_report AS stg
      WHERE substr(trim(stg.employee_id), 1, 1) NOT IN(
        '1', '2', '3', '4', '5', '6', '7', '8', '9'
      )
       OR stg.employee_id IS NULL
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

  TRUNCATE TABLE  {{ params.param_hr_stage_dataset_name }}.education_history_report_reject2 ;
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.education_history_report_reject2 (education_end_date)
    SELECT
        stg.education_end_date
      FROM
        {{ params.param_hr_stage_dataset_name }}.education_history_report AS stg
      WHERE substr(trim(stg.employee_id), 1, 1) IN(
        '1', '2', '3', '4', '5', '6', '7', '8', '9'
      )
       AND stg.employee_id IS NOT NULL
       AND substr(trim(stg.employee_id), 1, 1) IN(
        '1', '2', '3', '4', '5', '6', '7', '8', '9'
      )
       AND CASE
         trim(stg.employee_id)
        WHEN '' THEN 0
        ELSE CAST(trim(stg.employee_id) as INT64)
      END IN(
        SELECT
            employee_num AS employee_num
          FROM
            {{ params.param_hr_base_views_dataset_name }}.employee
      )
       AND trim(stg.education_end_date) = ''
  ;

  TRUNCATE TABLE  {{ params.param_hr_stage_dataset_name }}.education_history_report_reject3 ;
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.education_history_report_reject3 (education_end_date)
    SELECT
        stg.education_end_date
      FROM
        {{ params.param_hr_stage_dataset_name }}.education_history_report AS stg
      WHERE substr(trim(stg.employee_id), 1, 1) IN(
        '1', '2', '3', '4', '5', '6', '7', '8', '9'
      )
       AND stg.employee_id IS NOT NULL
       AND substr(trim(stg.employee_id), 1, 1) IN(
        '1', '2', '3', '4', '5', '6', '7', '8', '9'
      )
       AND CASE
         trim(stg.employee_id)
        WHEN '' THEN 0
        ELSE CAST(trim(stg.employee_id) as INT64)
      END IN(
        SELECT
            employee_num AS employee_num
          FROM
            {{ params.param_hr_base_views_dataset_name }}.employee
      )
       AND trim(stg.education_start_date) = ''
  ;

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.additional_education_history_reject;
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.additional_education_history_reject (employee_id, spoken_languages, written_languages, prof_org_free_text, licenses_certifications, skills_experience, special_training, passionate_job_functions, successful_job_functions, edu_hist_record_id, dw_last_update_date_time, reject_reason, reject_stg_tbl_nm)
    SELECT
        stg.employee_id,
        stg.spoken_languages,
        stg.written_languages,
        stg.prof_org_free_text,
        stg.licenses_certifications,
        stg.skills_experience,
        stg.special_training,
        stg.passionate_job_functions,
        stg.successful_job_functions,
        stg.edu_hist_record_id,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
        CASE
          WHEN trim(stg.employee_id) IS NULL THEN 'Employee_ID is NULL'
          WHEN substr(trim(stg.employee_id), 1, 1) NOT IN(
            '1', '2', '3', '4', '5', '6', '7', '8', '9'
          ) THEN 'Employee_Id is alpha_numeric'
          ELSE CAST(0 as STRING)
        END AS reject_reason,
        'Employee' AS reject_stg_tbl_nm
      FROM
        {{ params.param_hr_stage_dataset_name }}.additional_education_history AS stg
      WHERE substr(trim(stg.employee_id), 1, 1) NOT IN(
        '1', '2', '3', '4', '5', '6', '7', '8', '9'
      )
       OR stg.employee_id IS NULL
       OR substr(trim(stg.employee_id), 1, 1) IN(
        '1', '2', '3', '4', '5', '6', '7', '8', '9'
      )
       AND CASE
         trim(stg.employee_id)
        WHEN '' THEN 0
        ELSE CAST(trim(stg.employee_id) as INT64)
      END NOT IN(
        SELECT
            employee_num AS employee_num
          FROM
            {{ params.param_hr_base_views_dataset_name }}.employee
      )
  ;

  CALL `{{ params.param_hr_core_dataset_name }}.sk_gen`("{{ params.param_hr_stage_dataset_name }}","education_history_report","Trim(edu_hist_record_id)","Employee_Education_Profile");

  TRUNCATE TABLE  {{ params.param_hr_stage_dataset_name }}.employee_education_profile_wrk2;
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_education_profile_wrk2 (employee_education_profile_sid, employee_num, employee_sid, employee_talent_profile_sid, employee_education_type_code, detail_value_alpahnumeric_text, detail_value_num, detail_value_date, lawson_company_num, process_level_code, source_system_key, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(xwlk.sk AS INT64) AS employee_education_profile_sid,
        CAST(stg.employee_id AS INT64) AS employee_num,
        emp.employee_sid AS employee_sid,
        etp.employee_talent_profile_sid,
        '0' AS employee_education_type_code,
        '0' AS detail_value_alpahnumeric_text,
        0 AS detail_value_num,
        current_date('US/Central') AS detail_value_date,
        CAST((coalesce(etp.lawson_company_num, 0)) AS INT64) AS lawson_company_num,
        trim(CAST(coalesce(etp.process_level_code, "0") as STRING)) AS process_level_code,
        stg.edu_hist_record_id AS source_system_key,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_tim
      FROM
        {{ params.param_hr_stage_dataset_name }}.education_history_report AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON trim(stg.edu_hist_record_id) = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'EMPLOYEE_EDUCATION_PROFILE'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON CASE
          WHEN trim(stg.employee_id) IS NOT NULL
           AND substr(trim(stg.employee_id), 1, 1) IN(
            '1', '2', '3', '4', '5', '6', '7', '8', '9'
          ) THEN CASE
             trim(stg.employee_id)
            WHEN '' THEN 0
            ELSE CAST(trim(stg.employee_id) as INT64)
          END
          ELSE 0
        END = emp.employee_num
         AND (emp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND emp.lawson_company_num = CASE
           substr(stg.job_code, 1, 4)
          WHEN '' THEN 0
          ELSE CAST(substr(stg.job_code, 1, 4) as INT64)
        END
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_talent_profile AS etp ON CASE
          WHEN trim(stg.employee_id) IS NOT NULL
           AND substr(trim(stg.employee_id), 1, 1) IN(
            '1', '2', '3', '4', '5', '6', '7', '8', '9'
          ) THEN CASE
             trim(stg.employee_id)
            WHEN '' THEN 0
            ELSE CAST(trim(stg.employee_id) as INT64)
          END
          ELSE 0
        END = CASE WHEN
           (etp.employee_num) IS NULL
           THEN 0
          ELSE etp.employee_num
        END
         AND (etp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN (
          SELECT DISTINCT
              additional_education_history.employee_id
            FROM
              {{ params.param_hr_stage_dataset_name }}.additional_education_history
        ) AS stg2 ON CASE
          WHEN trim(stg2.employee_id) IS NOT NULL
           AND substr(trim(stg2.employee_id), 1, 1) IN(
            '1', '2', '3', '4', '5', '6', '7', '8', '9'
          ) THEN CASE
             trim(stg2.employee_id)
            WHEN '' THEN 0
            ELSE CAST(trim(stg2.employee_id) as INT64)
          END
          ELSE 0
        END = CASE 
           WHEN (etp.employee_num) IS NULL --trim(CAST (etp.employee_num AS STRING)) -- CHANGE trim(etp.employee_num)
           THEN 0
          ELSE  etp.employee_num
        END
  ;


  DROP TABLE  {{ params.param_hr_stage_dataset_name }}.employee_edu_hist_rpt_wrk ;

  CREATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_edu_hist_rpt_wrk
    AS
      SELECT
          xwlk.sk AS employee_education_profile_sid,
          stg.employee_id AS employee_num,
          emp.employee_sid AS employee_sid,
          etp.employee_talent_profile_sid,
          coalesce(stg.school_name,'') AS school_name,
          coalesce(stg.school_type,'') AS school_type,
          coalesce(stg.degree,'') AS degree,
          coalesce(stg.major,'') AS major,
          stg.education_start_date AS education_start_date,
          stg.education_end_date AS education_end_date,
          coalesce(stg.year_graduated,'') AS year_graduated,
          coalesce(stg.gpa,'') AS gpa,
          coalesce(stg.education_comments,'') AS education_comments,
          current_date('US/Central') AS detail_value_date,
          trim(CAST(coalesce(etp.lawson_company_num, 0) as STRING)) AS lawson_company_num,
          trim(coalesce(etp.process_level_code, '00000')) AS process_level_code,
          stg.edu_hist_record_id AS source_system_key,
          'M' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_tim
        FROM
          {{ params.param_hr_stage_dataset_name }}.education_history_report AS stg
          INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON trim(stg.edu_hist_record_id) = xwlk.sk_source_txt
           AND upper(xwlk.sk_type) = 'EMPLOYEE_EDUCATION_PROFILE'
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON CASE
            WHEN trim(stg.employee_id) IS NOT NULL
             AND substr(trim(stg.employee_id), 1, 1) IN(
              '1', '2', '3', '4', '5', '6', '7', '8', '9'
            ) THEN CASE
               trim(stg.employee_id)
              WHEN '' THEN 0
              ELSE CAST(trim(stg.employee_id) as INT64)
            END
            ELSE 0
          END = emp.employee_num
           AND (emp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
           AND emp.lawson_company_num = CASE
             substr(stg.job_code, 1, 4)
            WHEN '' THEN 0
            ELSE CAST(substr(stg.job_code, 1, 4) as INT64)
          END
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_talent_profile AS etp ON CASE
            WHEN trim(stg.employee_id) IS NOT NULL
             AND substr(trim(stg.employee_id), 1, 1) IN(
              '1', '2', '3', '4', '5', '6', '7', '8', '9'
            ) THEN CASE
               trim(stg.employee_id)
              WHEN '' THEN 0 
              ELSE CAST(trim(stg.employee_id) as INT64)
            END
            ELSE 0
          END = CASE
             WHEN (etp.employee_num) IS NULL
             THEN 0
            ELSE etp.employee_num 
          END
           AND (etp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
          LEFT OUTER JOIN (
            SELECT DISTINCT
                additional_education_history.employee_id
              FROM
                {{ params.param_hr_stage_dataset_name }}.additional_education_history
          ) AS stg2 ON CASE
            WHEN trim(stg2.employee_id) IS NOT NULL
             AND substr(trim(stg2.employee_id), 1, 1) IN(
              '1', '2', '3', '4', '5', '6', '7', '8', '9'
            ) THEN CASE
               trim(stg2.employee_id)
              WHEN '' THEN 0
              ELSE CAST(trim(stg2.employee_id) as INT64)
            END
            ELSE 0
          END = CASE
            WHEN etp.employee_num IS NULL
            THEN 0
            ELSE etp.employee_num
          END
  ;


  TRUNCATE TABLE  {{ params.param_hr_stage_dataset_name }}.employee_education_profile_wrk3 ;
    INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_education_profile_wrk3 (employee_education_profile_sid, employee_sid, employee_num, employee_talent_profile_sid, employee_education_type_code, detail_value_alpahnumeric_text, detail_value_num, detail_value_date, lawson_company_num, process_level_code, source_system_key, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(employee_edu_hist_rpt_wrk.employee_education_profile_sid AS INT64),
        employee_edu_hist_rpt_wrk.employee_sid,
        CAST(employee_edu_hist_rpt_wrk.employee_num AS INT64),
        employee_edu_hist_rpt_wrk.employee_talent_profile_sid,
        'School Name',
        CASE
          WHEN employee_edu_hist_rpt_wrk.school_name IS NOT NULL THEN employee_edu_hist_rpt_wrk.school_name
          WHEN trim(employee_edu_hist_rpt_wrk.school_name) <> '' THEN employee_edu_hist_rpt_wrk.school_name
        END AS school_name,
        0 AS detail_value_num,
        NULL AS detail_value_date,
        CAST(employee_edu_hist_rpt_wrk.lawson_company_num AS INT64),
        employee_edu_hist_rpt_wrk.process_level_code,
        employee_edu_hist_rpt_wrk.source_system_key AS source_system_key,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_edu_hist_rpt_wrk
      WHERE employee_edu_hist_rpt_wrk.school_name IS NOT NULL
    UNION ALL
    SELECT
        CAST(employee_edu_hist_rpt_wrk_0.employee_education_profile_sid AS INT64),
        employee_edu_hist_rpt_wrk_0.employee_sid,
        CAST(employee_edu_hist_rpt_wrk_0.employee_num AS INT64),
        employee_edu_hist_rpt_wrk_0.employee_talent_profile_sid,
        'School Type',
        CASE
          WHEN employee_edu_hist_rpt_wrk_0.school_type IS NOT NULL THEN employee_edu_hist_rpt_wrk_0.school_type
          WHEN trim(employee_edu_hist_rpt_wrk_0.school_type) <> '' THEN employee_edu_hist_rpt_wrk_0.school_type
        END AS school_type,
        0 AS detail_value_num,
        NULL AS detail_value_date,
        CAST(employee_edu_hist_rpt_wrk_0.lawson_company_num AS INT64),
        employee_edu_hist_rpt_wrk_0.process_level_code,
        employee_edu_hist_rpt_wrk_0.source_system_key AS source_system_key,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_edu_hist_rpt_wrk AS employee_edu_hist_rpt_wrk_0
      WHERE employee_edu_hist_rpt_wrk_0.school_type IS NOT NULL
       AND trim(employee_edu_hist_rpt_wrk_0.school_type) <> ''
    UNION ALL
    SELECT
        CAST(employee_edu_hist_rpt_wrk_1.employee_education_profile_sid AS INT64),
        employee_edu_hist_rpt_wrk_1.employee_sid,
        CAST(employee_edu_hist_rpt_wrk_1.employee_num AS INT64),
        employee_edu_hist_rpt_wrk_1.employee_talent_profile_sid,
        'Degree',
        employee_edu_hist_rpt_wrk_1.degree AS degree,
        0 AS detail_value_num,
        NULL AS detail_value_date,
        CAST(employee_edu_hist_rpt_wrk_1.lawson_company_num AS INT64),
        employee_edu_hist_rpt_wrk_1.process_level_code,
        employee_edu_hist_rpt_wrk_1.source_system_key AS source_system_key,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_edu_hist_rpt_wrk AS employee_edu_hist_rpt_wrk_1
      WHERE employee_edu_hist_rpt_wrk_1.degree IS NOT NULL
    UNION ALL
    SELECT
        CAST(employee_edu_hist_rpt_wrk_2.employee_education_profile_sid AS INT64),
        employee_edu_hist_rpt_wrk_2.employee_sid,
        CAST(employee_edu_hist_rpt_wrk_2.employee_num AS INT64),
        employee_edu_hist_rpt_wrk_2.employee_talent_profile_sid,
        'Major',
        employee_edu_hist_rpt_wrk_2.major AS major,
        0 AS detail_value_num,
        NULL AS detail_value_date,
        CAST(employee_edu_hist_rpt_wrk_2.lawson_company_num AS INT64),
        employee_edu_hist_rpt_wrk_2.process_level_code,
        employee_edu_hist_rpt_wrk_2.source_system_key AS source_system_key,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_edu_hist_rpt_wrk AS employee_edu_hist_rpt_wrk_2
      WHERE employee_edu_hist_rpt_wrk_2.major IS NOT NULL
    UNION ALL
    SELECT
        CAST(employee_edu_hist_rpt_wrk_3.employee_education_profile_sid AS INT64),
        employee_edu_hist_rpt_wrk_3.employee_sid,
        CAST(employee_edu_hist_rpt_wrk_3.employee_num AS INT64),
        employee_edu_hist_rpt_wrk_3.employee_talent_profile_sid,
        'Education Start Date',
        '',
        0 AS detail_value_num,
        parse_date('%m/%d/%Y',REGEXP_EXTRACT(employee_edu_hist_rpt_wrk_3.education_start_date, r'[0-9]+/[0-9]+/[0-9]+'))
        AS detail_value_date,
        CAST(employee_edu_hist_rpt_wrk_3.lawson_company_num AS INT64),
        employee_edu_hist_rpt_wrk_3.process_level_code,
        employee_edu_hist_rpt_wrk_3.source_system_key AS source_system_key,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_edu_hist_rpt_wrk AS employee_edu_hist_rpt_wrk_3
      WHERE trim(employee_edu_hist_rpt_wrk_3.education_start_date) <> ''
      AND employee_edu_hist_rpt_wrk_3.education_start_date IS NOT NULL
    UNION ALL
    SELECT
        CAST(employee_edu_hist_rpt_wrk_4.employee_education_profile_sid AS INT64),
        employee_edu_hist_rpt_wrk_4.employee_sid,
        CAST(employee_edu_hist_rpt_wrk_4.employee_num AS INT64),
        employee_edu_hist_rpt_wrk_4.employee_talent_profile_sid,
        'Education End Date',
        '',
        0 AS detail_value_num,
        parse_date('%m/%d/%Y',REGEXP_EXTRACT(employee_edu_hist_rpt_wrk_4.education_end_date, r'[0-9]+/[0-9]+/[0-9]+'))
        AS detail_value_date,
        CAST(employee_edu_hist_rpt_wrk_4.lawson_company_num AS INT64),
        employee_edu_hist_rpt_wrk_4.process_level_code,
        employee_edu_hist_rpt_wrk_4.source_system_key AS source_system_key,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_edu_hist_rpt_wrk AS employee_edu_hist_rpt_wrk_4
      WHERE trim(employee_edu_hist_rpt_wrk_4.education_end_date) <> ''
      AND employee_edu_hist_rpt_wrk_4.education_end_date IS NOT NULL
    UNION ALL
    SELECT
        CAST(employee_edu_hist_rpt_wrk_5.employee_education_profile_sid AS INT64),
        employee_edu_hist_rpt_wrk_5.employee_sid,
        CAST(employee_edu_hist_rpt_wrk_5.employee_num AS INT64),
        employee_edu_hist_rpt_wrk_5.employee_talent_profile_sid,
        'Year Graduated',
        employee_edu_hist_rpt_wrk_5.year_graduated AS year_graduated,
        0 AS detail_value_num,
        NULL AS detail_value_date,
        CAST(employee_edu_hist_rpt_wrk_5.lawson_company_num AS INT64),
        employee_edu_hist_rpt_wrk_5.process_level_code,
        employee_edu_hist_rpt_wrk_5.source_system_key AS source_system_key,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_edu_hist_rpt_wrk AS employee_edu_hist_rpt_wrk_5
      WHERE employee_edu_hist_rpt_wrk_5.year_graduated IS NOT NULL
    UNION ALL
    SELECT
        CAST(employee_edu_hist_rpt_wrk_6.employee_education_profile_sid AS INT64),
        employee_edu_hist_rpt_wrk_6.employee_sid,
        CAST(employee_edu_hist_rpt_wrk_6.employee_num AS INT64),
        employee_edu_hist_rpt_wrk_6.employee_talent_profile_sid,
        'GPA',
        employee_edu_hist_rpt_wrk_6.gpa AS gpa,
        0 AS detail_value_num,
        NULL AS detail_value_date,
        CAST(employee_edu_hist_rpt_wrk_6.lawson_company_num AS INT64),
        employee_edu_hist_rpt_wrk_6.process_level_code,
        employee_edu_hist_rpt_wrk_6.source_system_key AS source_system_key,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_edu_hist_rpt_wrk AS employee_edu_hist_rpt_wrk_6
      WHERE employee_edu_hist_rpt_wrk_6.gpa IS NOT NULL
    UNION ALL
    SELECT
        CAST(employee_edu_hist_rpt_wrk_7.employee_education_profile_sid AS INT64),
        employee_edu_hist_rpt_wrk_7.employee_sid,
        CAST(employee_edu_hist_rpt_wrk_7.employee_num AS INT64),
        employee_edu_hist_rpt_wrk_7.employee_talent_profile_sid,
        'Education Comments',
        employee_edu_hist_rpt_wrk_7.education_comments AS education_comments,
        0 AS detail_value_num,
        NULL AS detail_value_date,
        CAST(employee_edu_hist_rpt_wrk_7.lawson_company_num AS INT64),
        employee_edu_hist_rpt_wrk_7.process_level_code,
        employee_edu_hist_rpt_wrk_7.source_system_key AS source_system_key,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_edu_hist_rpt_wrk AS employee_edu_hist_rpt_wrk_7
      WHERE employee_edu_hist_rpt_wrk_7.education_comments IS NOT NULL
      AND trim(employee_edu_hist_rpt_wrk_7.education_comments) <> ''
  ;


  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_education_profile_wrk;
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_education_profile_wrk 
    SELECT
        employee_education_profile_wrk3.*
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_education_profile_wrk3
      QUALIFY row_number() OVER (PARTITION BY employee_education_profile_wrk3.employee_education_profile_sid, employee_education_profile_wrk3.employee_education_type_code, employee_education_profile_wrk3.employee_talent_profile_sid ORDER BY employee_education_profile_wrk3.employee_education_profile_sid) = 1
  ;


BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt datetime;
  SET current_dt = datetime_trunc(current_datetime('US/Central'), SECOND);

  BEGIN TRANSACTION;
  UPDATE {{ params.param_hr_core_dataset_name }}.employee_education_profile AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) FROM (
    SELECT
        *
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_education_profile_wrk
  ) AS wrk WHERE (tgt.employee_education_profile_sid) = (wrk.employee_education_profile_sid)
   AND upper(trim(coalesce(tgt.employee_education_type_code, ''))) = upper(trim(coalesce(wrk.employee_education_type_code, '')))
   AND (trim(CAST(coalesce(tgt.employee_talent_profile_sid, 0) as STRING)) <> trim(CAST(coalesce(wrk.employee_talent_profile_sid, 0) as STRING))
   OR trim(CAST(coalesce(tgt.employee_sid, 0) as STRING)) <> trim(CAST(coalesce(wrk.employee_sid, 0) as STRING))
   OR trim(CAST(coalesce(tgt.employee_num, 0) as STRING)) <> trim(CAST(coalesce(wrk.employee_num, 0) as STRING))
   OR upper(trim(coalesce(tgt.detail_value_alpahnumeric_text, ''))) <> upper(trim(coalesce(wrk.detail_value_alpahnumeric_text, '')))
   OR trim(CAST(coalesce(tgt.detail_value_num, 0) as STRING)) <> trim(CAST(coalesce(wrk.detail_value_num, 0) as STRING))
   OR (coalesce(tgt.detail_value_date, DATE '9999-01-01')) <> (coalesce(wrk.detail_value_date, DATE '9999-01-01'))
   OR trim(CAST(coalesce(tgt.lawson_company_num, 0) as STRING)) <> trim(CAST(coalesce(wrk.lawson_company_num, 0) as STRING))
   OR ((coalesce(tgt.process_level_code, "0") )) <> ((coalesce(wrk.process_level_code, "0") ))
   OR upper(trim(coalesce(tgt.source_system_key, ''))) <> upper(trim(coalesce(wrk.source_system_key, '')))
   OR trim(CAST(coalesce(tgt.source_system_code, "0") as STRING)) <> trim(CAST(coalesce(wrk.source_system_code, "0") as STRING)))
   AND (tgt.valid_to_date) =  DATETIME("9999-12-31 23:59:59");


/*Core Table Load*/
  INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_education_profile (employee_education_profile_sid, employee_sid, employee_num, employee_education_type_code, valid_from_date, employee_talent_profile_sid, detail_value_alpahnumeric_text, detail_value_num, detail_value_date, lawson_company_num, process_level_code, valid_to_date, source_system_key, source_system_code, dw_last_update_date_time)
    SELECT
        wrk.employee_education_profile_sid,
        wrk.employee_sid,
        wrk.employee_num,
        trim(wrk.employee_education_type_code),
        current_dt AS valid_from_date,
        wrk.employee_talent_profile_sid,
        trim(wrk.detail_value_alpahnumeric_text),
        wrk.detail_value_num,
        (wrk.detail_value_date),
        wrk.lawson_company_num,
        wrk.process_level_code,
        DATETIME("9999-12-31 23:59:59")  AS valid_to_date,
        TRIM(wrk.source_system_key),
        wrk.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_education_profile_wrk AS wrk
      WHERE ( coalesce(wrk.employee_education_profile_sid,0),
              coalesce(wrk.employee_sid,0), 
              coalesce(wrk.employee_num,0), 
              coalesce(trim(wrk.employee_education_type_code),''), 
              coalesce(wrk.employee_talent_profile_sid,0), 
              upper(coalesce(trim(wrk.detail_value_alpahnumeric_text),'')), 
              coalesce(wrk.detail_value_num,0), 
              coalesce(wrk.detail_value_date, DATE '9999-01-01'),
              coalesce(wrk.lawson_company_num,0), 
              coalesce(Trim(wrk.process_level_code),''),
              upper(coalesce(Trim(wrk.source_system_key),'')), 
              coalesce(Trim(wrk.source_system_code),'')
            ) 
            NOT IN
            (
              SELECT AS STRUCT
              coalesce(tgt.employee_education_profile_sid,0),
              coalesce(tgt.employee_sid,0),
              coalesce(tgt.employee_num,0),
              coalesce(trim(tgt.employee_education_type_code),''),
              coalesce(tgt.employee_talent_profile_sid,0),
              upper(coalesce(trim(tgt.detail_value_alpahnumeric_text),'')),
              coalesce(tgt.detail_value_num,0),
              coalesce(tgt.detail_value_date, DATE '9999-01-01'),
              coalesce(tgt.lawson_company_num,0),
              coalesce(trim(tgt.process_level_code),''),
              upper(coalesce(trim(tgt.source_system_key),'')),
              coalesce(trim(tgt.source_system_code),'')
              FROM
                {{ params.param_hr_base_views_dataset_name }}.employee_education_profile AS tgt
              WHERE (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59")
            );

        /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Employee_Education_Profile_SID ,Employee_Education_Type_Code ,Valid_From_Date 
        from {{ params.param_hr_core_dataset_name }}.employee_education_profile
        group by Employee_Education_Profile_SID ,Employee_Education_Type_Code ,Valid_From_Date 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.employee_education_profile');
    ELSE
      COMMIT TRANSACTION;
    END IF;

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_education_profile AS tgt SET valid_to_date =  current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHERE tgt.valid_to_date =  DATETIME("9999-12-31 23:59:59")
   AND tgt.employee_education_profile_sid NOT IN(
    SELECT DISTINCT
        wrk.employee_education_profile_sid
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_education_profile_wrk AS wrk
  );
END;
