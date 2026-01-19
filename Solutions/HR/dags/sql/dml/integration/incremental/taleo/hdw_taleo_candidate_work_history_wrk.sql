BEGIN
  DECLARE DUP_COUNT INT64;
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.taleo_candidate_work_history_wrk;


BEGIN TRANSACTION;

  /*  Generate the surrogate keys for Candidate */
  CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_experience', 'CAST(taleo_number AS STRING)', 'Candidate_Work_History');
  /*  Generate the surrogate keys for ATS Candidate */
  CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'ats_edw_candidateemploymenthistory_stg', 'Trim(UniqueId)', 'Candidate_Work_History');
  
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.taleo_candidate_work_history_wrk (file_date, candidate_work_history_sid, candidate_work_history_num, candidate_profile_sid, candidate_sid, work_start_date, work_end_date, current_employer_sw, profile_display_seq_num, employer_name, job_title_name, source_system_code, dw_last_update_date_time)
    SELECT
        stg.file_date AS file_date,
        CAST(xwlk.sk as INT64) AS candidate_work_history_sid,
        stg.taleo_number AS candidate_work_history_num,
        cwh.candidate_profile_sid AS candidate_profile_sid,
        cwh1.candidate_sid AS candidate_sid,
        stg.begindate AS work_start_date,
        stg.enddate AS work_end_date,
        CAST(stg.currentemployer as INT64) AS current_employer_sw,
        CAST(stg.displaysequence as INT64) AS profile_display_seq_num,
        stg.otheremployername AS employer_name,
        stg.otherjobtitle AS job_title_name,
        'T' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_experience AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cwh ON stg.profileinformation_number = cwh.candidate_profile_num
         AND cwh.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(cwh.source_system_code) = 'T'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cwh1 ON stg.taleo_number = cwh1.candidate_profile_num
         AND cwh1.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(cwh1.source_system_code) = 'T'
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(substr(trim(CAST(stg.taleo_number as STRING)), 1, 255)) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_WORK_HISTORY'
      WHERE upper(CAST(taleo_number as STRING)) LIKE '1%'
       OR upper(CAST(taleo_number as STRING)) LIKE '2%'
       OR upper(CAST(taleo_number as STRING)) LIKE '3%'
       OR upper(CAST(taleo_number as STRING)) LIKE '4%'
       OR upper(CAST(taleo_number as STRING)) LIKE '5%'
       OR upper(CAST(taleo_number as STRING)) LIKE '6%'
       OR upper(CAST(taleo_number as STRING)) LIKE '7%'
       OR upper(CAST(taleo_number as STRING)) LIKE '8%'
       OR upper(CAST(taleo_number as STRING)) LIKE '9%'
    UNION DISTINCT
    SELECT
        CURRENT_DATE('US/Central') AS file_date,
        CAST(xwlk.sk as INT64) AS candidate_work_history_sid,
        CASE
           concat(stg.candidate, stg.sequencenumber)
          WHEN '' THEN 0
          ELSE CAST(concat(stg.candidate, stg.sequencenumber) as INT64)
        END AS candidate_work_history_num,
        NULL AS candidate_profile_sid,
        cwh1.candidate_sid AS candidate_sid,
        CASE
          WHEN stg.begindate_year <> 0
           AND stg.begindate_month <> 0 THEN parse_date('%Y-%m-%d', concat(stg.begindate_year, '-', 
           CASE
            WHEN length(cast(stg.begindate_month as string)) = 1 THEN concat('0', stg.begindate_month)
            ELSE CAST(stg.begindate_month AS STRING)
          END, '-', '01'))
          ELSE CAST(NULL as DATE) 
        END  AS work_start_date,
        CASE
          WHEN stg.enddate_year <> 0
           AND stg.enddate_month <> 0 THEN parse_date('%Y-%m-%d', concat(stg.enddate_year, '-', CASE
            WHEN length(trim(CAST(stg.enddate_month as STRING))) = 1 THEN concat('0', stg.enddate_month)
            ELSE CAST(stg.enddate_month as STRING)
          END, '-', '01'))
          ELSE CAST(NULL as DATE)
        END AS work_end_date,
        CASE
          WHEN CASE
             trim(CAST(stg.enddate_month as STRING))
            WHEN '' THEN 0.0
            ELSE CAST(trim(CAST(stg.enddate_month as STRING)) as FLOAT64)
          END = 0
           AND stg.enddate_year = 0 THEN 1
          ELSE 0
        END AS current_employer_sw ,
        CAST(stg.sequencenumber as INT64) AS profile_display_seq_num,
        trim(stg.employername) AS employer_name,
        trim(stg.jobtitle) AS job_title_name,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_edw_candidateemploymenthistory_stg AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS cwh1 ON stg.candidate = cwh1.candidate_num
         AND cwh1.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(cwh1.source_system_code) = 'B'
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(substr(trim(stg.uniqueid), 1, 255)) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_WORK_HISTORY'
      QUALIFY row_number() OVER (PARTITION BY stg.uniqueid ORDER BY stg.update_stamp_timestamp DESC) = 1
  ;
  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT candidate_work_history_sid 
      FROM {{ params.param_hr_stage_dataset_name }}.taleo_candidate_work_history_wrk
      GROUP BY candidate_work_history_sid 
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table:{{ params.param_hr_stage_dataset_name }}.taleo_candidate_work_history_wrk');
  
  ELSE
    COMMIT TRANSACTION;
  END IF;





   INSERT INTO {{ params.param_hr_stage_dataset_name }}.taleo_experience_reject 
    SELECT
        taleo_experience.*
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_experience
      WHERE upper(CAST(taleo_experience.taleo_number as STRING)) NOT LIKE '1%'
       AND upper(CAST(taleo_experience.taleo_number as STRING)) NOT LIKE '2%'
       AND upper(CAST(taleo_experience.taleo_number as STRING)) NOT LIKE '3%'
       AND upper(CAST(taleo_experience.taleo_number as STRING)) NOT LIKE '4%'
       AND upper(CAST(taleo_experience.taleo_number as STRING)) NOT LIKE '5%'
       AND upper(CAST(taleo_experience.taleo_number as STRING)) NOT LIKE '6%'
       AND upper(CAST(taleo_experience.taleo_number as STRING)) NOT LIKE '7%'
       AND upper(CAST(taleo_experience.taleo_number as STRING)) NOT LIKE '8%'
       AND upper(CAST(taleo_experience.taleo_number as STRING)) NOT LIKE '9%'
  ;
 
  END;