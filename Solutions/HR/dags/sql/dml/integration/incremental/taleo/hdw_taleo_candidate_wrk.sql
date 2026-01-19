/*  Generate the surrogate keys for Candidate */
CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_candidate', '(NUMBER)', 'CANDIDATE');

/*  Truncate Worktable Table     */

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_wrk;

/*  Load Work Table with working Data */

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_wrk (file_date, candidate_sid, valid_from_date, candidate_num, in_hiring_process_sw, internal_candidate_sw, referred_sw, last_modified_date_time, candidate_creation_date_time, residence_location_num, travel_preference_code, relocation_preference_code, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        -- 	CAST ( TRIM(STG."NUMBER") AS INTEGER )  AS  CANDIDATE_SID
        stg.file_date AS file_date,
        CAST(xwlk.sk AS INT64) AS candidate_sid,
        stg.file_date AS valid_from_date,
        CASE
           trim(stg.number)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.number) as INT64)
        END AS candidate_num,
        CASE
           trim(stg.inhiringprocess)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.inhiringprocess) as INT64)
        END AS in_hiring_process_sw,
        CASE
           trim(stg.internalapplication)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.internalapplication) as INT64)
        END AS internal_candidate_sw,
        CASE
           trim(stg.referred)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.referred) as INT64)
        END AS referred_sw,
        DATETIME_TRUNC(CAST(stg.lastmodifieddate AS DATETIME),SECOND),
        DATETIME_TRUNC(CAST(stg.creationdate AS DATETIME),SECOND),
       -------- -- CAST(stg.creationdate as TIMESTAMP) + (stg.creationdate - TIME '00:00:00'),
        -- ,	CAST ( TRIM(STG.LASTMODIFIEDDATE) AS TIMESTAMP(0) ) AS LAST_MODIFIED_DATE_TIME
        -- ,	CAST ( TRIM(STG.CREATIONDATE) AS TIMESTAMP(0) ) AS CANDIDATE_CREATION_DATE_TIME
        -- ,	STG.LASTMODIFIEDDATE AS LAST_MODIFIED_DATE_TIME
        -- ,	STG.CREATIONDATE AS CANDIDATE_CREATION_DATE_TIME
        CASE
           trim(stg.residencelocation_number)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.residencelocation_number) as INT64)
        END AS residence_location_num,
        CAST(NULL AS STRING) AS travel_preference_code,
        CAST(NULL AS STRING) relocation_preference_code,
        DATETIME '9999-12-31' AS valid_to_date,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_candidate AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(substr(trim(stg.number), 1, 255)) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
  ;


-- Adeed below code as part of ATS developemnt HDM-1310
CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'ats_cust_v2_candidate_stg', '((CANDIDATE))||\'-ATS\'', 'CANDIDATE');

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_wrk (file_date, candidate_sid, valid_from_date, candidate_num, in_hiring_process_sw, internal_candidate_sw, referred_sw, last_modified_date_time, candidate_creation_date_time, residence_location_num, travel_preference_code, relocation_preference_code, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        CURRENT_DATE('US/Central') AS file_date,
        CASt(xwlk.sk AS INT64) AS candidate_sid,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
        CAST(stg.candidate as INT64) AS candidate_num,
        CASE
          WHEN stg1.status IN(
            1, 2, 3, 6
          ) THEN 1
          ELSE 0
        END AS in_hiring_process_sw,
        CASE
          WHEN UPPER(trim(stg.type_state)) = 'INTERNAL' THEN 1
          ELSE 0
        END AS internal_candidate_sw,
        CASE
          WHEN stg3.candidate IS NOT NULL
           OR stg2.candidate IS NOT NULL THEN 1
          ELSE 0
        END AS referred_sw,
        coalesce(CAST(stg.infor_lastmodified as DATETIME), DATETIME '0001-01-01 00:00:00') AS last_modified_date_time,
        coalesce(CAST(stg.createstamp as DATETIME), DATETIME '0001-01-01 00:00:00') AS candidate_creation_date_time,
        NULL AS residence_location_num,
        trim(stg.recruitingpreferences_preferences_travel_state) AS travel_preference_code,
        trim(stg.recruitingpreferences_preferences_relocate_state) AS relocation_preference_code,
        DATETIME '9999-12-31 23:59:59' AS valid_to_date,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_candidate_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(trim(concat(stg.candidate, '-ATS'))) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE'
        LEFT OUTER JOIN (
          SELECT
              ats_cust_v3_jobapplication_stg.candidate,
              ats_cust_v3_jobapplication_stg.status,
              ats_cust_v3_jobapplication_stg.infor_lastmodified
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg
            QUALIFY row_number() OVER (PARTITION BY ats_cust_v3_jobapplication_stg.candidate ORDER BY ats_cust_v3_jobapplication_stg.infor_lastmodified DESC) = 1
        ) AS stg1 ON stg1.candidate = CAST(stg.candidate as INT64)
        LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ats_edw_candidatereference_stg AS stg2 ON stg2.candidate = CAST(stg.candidate as INT64)
        LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ats_edw_jobapplicationreference_stg AS stg3 ON stg3.candidate = CAST(stg.candidate as INT64)
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
      QUALIFY row_number() OVER (PARTITION BY candidate_sid ORDER BY (last_modified_date_time) DESC) = 1
  ;




