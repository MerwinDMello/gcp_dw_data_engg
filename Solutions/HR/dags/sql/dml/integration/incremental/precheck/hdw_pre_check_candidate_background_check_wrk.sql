
CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'precheck_stg', 'TRIM(CAST(REPORT_NUMBER AS STRING))||\'-PRE\'', 'CANDIDATE_BACKGROUND_CHECK');

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_background_check_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_background_check_wrk (report_sid, valid_from_date, report_create_date_time, candidate_first_name, candidate_middle_name, candidate_last_name, social_security_num, report_reopen_date_time, report_completion_date_time, process_level_code, recruitment_requisition_num_text, days_elapsed_cnt, criminal_search_ordered_cnt, criminal_search_pending_cnt, motor_vehicle_record_ordered_cnt, motor_vehicle_record_pending_cnt, employment_verification_ordered_cnt, employment_verification_pending_cnt, education_verification_ordered_cnt, education_verification_pending_cnt, license_verification_ordered_cnt, license_verification_pending_cnt, personal_reference_ordered_cnt, personal_reference_pending_cnt, sanction_check_ordered_cnt, sanction_check_pending_cnt, report_num, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(xwlk.sk as int64) AS report_sid,
        datetime_trunc(current_datetime('US/Central'), second) AS valid_from_date,
        CAST(CASE
          WHEN trim(stg.report_created_date) <> '' THEN parse_timestamp('%m/%d/%Y %I:%M:%S %p', CASE
            WHEN substr(stg.report_created_date, 2, 1) = '/'
             AND substr(stg.report_created_date, 4, 1) = '/'
             AND substr(stg.report_created_date, 11, 1) = ':' THEN concat('0', substr(stg.report_created_date, 1, 2), '0', substr(stg.report_created_date, 3, 7), '0', substr(stg.report_created_date, 10))
            WHEN substr(stg.report_created_date, 2, 1) = '/'
             AND substr(stg.report_created_date, 4, 1) = '/'
             AND substr(stg.report_created_date, 12, 1) = ':' THEN concat('0', substr(stg.report_created_date, 1, 2), '0', substr(stg.report_created_date, 3))
            WHEN substr(stg.report_created_date, 2, 1) = '/'
             AND substr(stg.report_created_date, 5, 1) = '/'
             AND substr(stg.report_created_date, 12, 1) = ':' THEN concat('0', substr(stg.report_created_date, 1, 10), '0', substr(stg.report_created_date, 11))
            WHEN substr(stg.report_created_date, 2, 1) = '/'
             AND substr(stg.report_created_date, 5, 1) = '/'
             AND substr(stg.report_created_date, 13, 1) = ':' THEN concat('0', substr(stg.report_created_date, 1))
            WHEN substr(stg.report_created_date, 3, 1) = '/'
             AND substr(stg.report_created_date, 5, 1) = '/'
             AND substr(stg.report_created_date, 12, 1) = ':' THEN concat(substr(stg.report_created_date, 1, 3), '0', substr(stg.report_created_date, 4, 7), '0', substr(stg.report_created_date, 11))
            WHEN substr(stg.report_created_date, 3, 1) = '/'
             AND substr(stg.report_created_date, 5, 1) = '/'
             AND substr(stg.report_created_date, 13, 1) = ':' THEN concat(substr(stg.report_created_date, 1, 3), '0', substr(stg.report_created_date, 4))
            WHEN substr(stg.report_created_date, 3, 1) = '/'
             AND substr(stg.report_created_date, 6, 1) = '/'
             AND substr(stg.report_created_date, 13, 1) = ':' THEN concat(substr(stg.report_created_date, 1, 11), '0', substr(stg.report_created_date, 12))
            ELSE stg.report_created_date
          END)
          ELSE CAST(NULL as TIMESTAMP)
        END as datetime) AS report_create_date_time,
        stg.applicant_first_name AS candidate_first_name,
        stg.applicant_middle_name AS candidate_middle_name,
        stg.applicant_last_name AS applicant_last_name,
        stg.ssn AS social_security_num,
        CAST(CASE
          WHEN trim(stg.report_reopened_date) <> '' THEN parse_timestamp('%m/%d/%Y %I:%M:%S %p', CASE
            WHEN substr(stg.report_reopened_date, 2, 1) = '/'
             AND substr(stg.report_reopened_date, 4, 1) = '/'
             AND substr(stg.report_reopened_date, 11, 1) = ':' THEN concat('0', substr(stg.report_reopened_date, 1, 2), '0', substr(stg.report_reopened_date, 3, 7), '0', substr(stg.report_reopened_date, 10))
            WHEN substr(stg.report_reopened_date, 2, 1) = '/'
             AND substr(stg.report_reopened_date, 4, 1) = '/'
             AND substr(stg.report_reopened_date, 12, 1) = ':' THEN concat('0', substr(stg.report_reopened_date, 1, 2), '0', substr(stg.report_reopened_date, 3))
            WHEN substr(stg.report_reopened_date, 2, 1) = '/'
             AND substr(stg.report_reopened_date, 5, 1) = '/'
             AND substr(stg.report_reopened_date, 12, 1) = ':' THEN concat('0', substr(stg.report_reopened_date, 1, 10), '0', substr(stg.report_reopened_date, 11))
            WHEN substr(stg.report_reopened_date, 2, 1) = '/'
             AND substr(stg.report_reopened_date, 5, 1) = '/'
             AND substr(stg.report_reopened_date, 13, 1) = ':' THEN concat('0', substr(stg.report_reopened_date, 1))
            WHEN substr(stg.report_reopened_date, 3, 1) = '/'
             AND substr(stg.report_reopened_date, 5, 1) = '/'
             AND substr(stg.report_reopened_date, 12, 1) = ':' THEN concat(substr(stg.report_reopened_date, 1, 3), '0', substr(stg.report_reopened_date, 4, 7), '0', substr(stg.report_reopened_date, 11))
            WHEN substr(stg.report_reopened_date, 3, 1) = '/'
             AND substr(stg.report_reopened_date, 5, 1) = '/'
             AND substr(stg.report_reopened_date, 13, 1) = ':' THEN concat(substr(stg.report_reopened_date, 1, 3), '0', substr(stg.report_reopened_date, 4))
            WHEN substr(stg.report_reopened_date, 3, 1) = '/'
             AND substr(report_reopened_date, 6, 1) = '/'
             AND substr(stg.report_reopened_date, 13, 1) = ':' THEN concat(substr(stg.report_reopened_date, 1, 11), '0', substr(stg.report_reopened_date, 12))
            ELSE stg.report_reopened_date
          END)
          ELSE CAST(NULL as TIMESTAMP)
        END as datetime) AS report_reopen_date_time,
        CAST(CASE
          WHEN trim(stg.report_completion_date) <> '' THEN parse_timestamp('%m/%d/%Y %I:%M:%S %p', CASE
            WHEN substr(stg.report_completion_date, 2, 1) = '/'
             AND substr(stg.report_completion_date, 4, 1) = '/'
             AND substr(stg.report_completion_date, 11, 1) = ':' THEN concat('0', substr(stg.report_completion_date, 1, 2), '0', substr(stg.report_completion_date, 3, 7), '0', substr(stg.report_completion_date, 10))
            WHEN substr(stg.report_completion_date, 2, 1) = '/'
             AND substr(stg.report_completion_date, 4, 1) = '/'
             AND substr(stg.report_completion_date, 12, 1) = ':' THEN concat('0', substr(stg.report_completion_date, 1, 2), '0', substr(stg.report_completion_date, 3))
            WHEN substr(stg.report_completion_date, 2, 1) = '/'
             AND substr(stg.report_completion_date, 5, 1) = '/'
             AND substr(stg.report_completion_date, 12, 1) = ':' THEN concat('0', substr(stg.report_completion_date, 1, 10), '0', substr(stg.report_completion_date, 11))
            WHEN substr(stg.report_completion_date, 2, 1) = '/'
             AND substr(stg.report_completion_date, 5, 1) = '/'
             AND substr(stg.report_completion_date, 13, 1) = ':' THEN concat('0', substr(stg.report_completion_date, 1))
            WHEN substr(stg.report_completion_date, 3, 1) = '/'
             AND substr(stg.report_completion_date, 5, 1) = '/'
             AND substr(stg.report_completion_date, 12, 1) = ':' THEN concat(substr(stg.report_completion_date, 1, 3), '0', substr(stg.report_completion_date, 4, 7), '0', substr(stg.report_completion_date, 11))
            WHEN substr(stg.report_completion_date, 3, 1) = '/'
             AND substr(stg.report_completion_date, 5, 1) = '/'
             AND substr(stg.report_completion_date, 13, 1) = ':' THEN concat(substr(stg.report_completion_date, 1, 3), '0', substr(stg.report_completion_date, 4))
            WHEN substr(stg.report_completion_date, 3, 1) = '/'
             AND substr(report_completion_date, 6, 1) = '/'
             AND substr(stg.report_completion_date, 13, 1) = ':' THEN concat(substr(stg.report_completion_date, 1, 11), '0', substr(stg.report_completion_date, 12))
            ELSE stg.report_completion_date
          END)
          ELSE CAST(NULL as TIMESTAMP)
        END as datetime) AS report_completion_date_time,
        CASE
          WHEN trim(stg.processlevel) IS NULL
           OR trim(stg.processlevel) = '' THEN '00000'
          ELSE concat(substr('00000', 1, 5 - length(trim(stg.processlevel))), trim(stg.processlevel))
        END AS process_level_code,
        stg.requisition AS recruitment_requisition_num_text,
        CAST(stg.elapsed_days  AS INT64) AS days_elapsed_cnt,
        CAST(stg.criminal_searches_ordered AS INT64) AS criminal_search_ordered_cnt,
        CAST(stg.criminal_searches_pending AS INT64) AS criminal_search_pending_cnt,
        CAST(stg.mvr_ordered AS INT64) AS motor_vehicle_record_ordered_cnt,
        CAST(stg.mvr_pending AS INT64) AS motor_vehicle_record_pending_cnt,
        CAST(stg.emp_verifications_ordered as int64) AS employment_verification_ordered_cnt,
        CAST(stg.emp_verifications_pending AS INT64) AS employment_verification_pending_cnt,
        CAST(stg.edu_verifications_ordered AS INT64) AS education_verification_ordered_cnt,
        CAST(stg.edu_verifications_pending AS INT64) AS education_verification_pending_cnt,
        CAST(stg.license_verifications_ordered AS INT64) AS license_verification_ordered_cnt,
        CAST(stg.license_verifications_pending AS INT64) AS license_verification_pending_cnt,
        CAST(stg.personal_references_ordered AS INT64) AS personal_reference_ordered_cnt,
        CAST(stg.personal_references_pending AS INT64) AS personal_reference_pending_cnt,
        CAST(stg.sanctioncheck_ordered AS INT64) AS sanction_check_ordered_cnt,
        CAST(stg.sanctioncheck_pending AS INT64) AS sanction_check_pending_cnt,
        stg.report_number AS report_num,
        DATETIME('9999-12-31 23:59:59') AS valid_to_date,
        'P' AS source_system_code,
        timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.precheck_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(trim(concat(stg.report_number, '-PRE'))) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_BACKGROUND_CHECK'
      QUALIFY row_number() OVER (PARTITION BY report_sid ORDER BY stg.report_status) = 1
  ;