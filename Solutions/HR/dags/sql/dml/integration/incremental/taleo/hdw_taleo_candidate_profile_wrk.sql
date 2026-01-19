/*  Generate the surrogate keys for Candidate */
CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_profileinformation', 'CAST(NUMBER_ AS STRING)', 'CANDIDATE_PROFILE');
CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'ats_cust_v3_jobapplication_stg', '(JobApplication)||(Candidate)||(JobRequisition)||\'-ATS\'', 'CANDIDATE_PROFILE');

/*  Generate the surrogate keys for Candidate */
-- CALL EDWHR_PROCS.SK_GEN('EDWHR_STAGING','TALEO_PROFILEINFORMATION','TRIM(CANDIDATE_NUMBER)', 'CANDIDATE');
-- .IF ERRORCODE <> 0 Then .Quit ERRORCODE;
/*  Truncate Worktable Table     */

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_profile_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_profile_wrk (candidate_profile_sid, valid_from_date, candidate_sid, profile_medium_id, candidate_profile_num, submission_date, submission_date_time, completion_date, completion_date_time, creation_date, creation_date_time, recruitment_source_id, recruitment_source_auto_filled_sw, job_application_num, requisition_num, candidate_num, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        stg.candidate_profile_sid ,
        stg.valid_from_date,
        stg.candidate_sid,
        stg.profile_medium_id,
        stg.candidate_profile_num,
        stg.submission_date,
		stg.submission_date_time,
        stg.completion_date,
		stg.completion_date_time,
        stg.creation_date,
		stg.creation_date_time,
        stg.recruitment_source_id,
        stg.recruitment_source_auto_filled_sw,
        stg.job_application_num,
        stg.requisition_num,
        stg.candidate_num,
        stg.valid_to_date,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        (
          SELECT
              cast(xwlk.sk as int64) AS candidate_profile_sid,
              current_date() AS valid_from_date,
              can.candidate_sid,
              CAST(NULL as INT64) AS profile_medium_id,
              CAST(concat((jobapplication), (candidate), (jobrequisition)) AS NUMERIC) AS candidate_profile_num,
              DATE(stg_0.applicationdate) AS submission_date,
			  coalesce(CAST(stg_0.applicationdate AS datetime), CAST('1901-01-01 00:00:00' AS datetime)) AS submission_date_time,
              DATE(stg_0.applicationdate) AS completion_date,
			  coalesce(CAST(stg_0.applicationdate AS datetime), CAST('1901-01-01 00:00:00' AS datetime)) AS completion_date_time,
              coalesce(DATE(updatestamp), DATE('1901-01-01'))AS last_modified_date_time,
              coalesce(DATE(createstamp), DATE('1901-01-01')) AS creation_date,
			  coalesce(CAST(createstamp AS datetime), CAST('1901-01-01 00:00:00' AS datetime)) AS creation_date_time,
              -- ,	CASE WHEN CREATESTAMP IS NULL THEN CAST( '1901-01-01' AS DATE FORMAT 'YYYY-MM-DD' ) ELSE  CAST(CREATESTAMP AS DATE FORMAT 'YYYY-MM-DD' ) END AS CREATION_DATE
              recruitment_source_id AS recruitment_source_id,
              CAST(NULL as INT64) AS recruitment_source_auto_filled_sw,
              CASE
                 (stg_0.jobapplication)
                WHEN NULL THEN 0
                ELSE CAST((stg_0.jobapplication) as INT64)
              END AS job_application_num,
              CASE
                 (stg_0.jobrequisition)
                WHEN NULL THEN 0
                ELSE CAST((stg_0.jobrequisition) as INT64)
              END AS requisition_num,
              CASE
                 (stg_0.candidate)
                WHEN NULL THEN 0
                ELSE CAST((stg_0.candidate) as INT64)
              END AS candidate_num,
              DATETIME('9999-12-31 23:59:59') AS valid_to_date,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg_0
              INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat((stg_0.jobapplication), (candidate), (jobrequisition), '-ATS') = xwlk.sk_source_txt
               AND upper(xwlk.sk_type) = 'CANDIDATE_PROFILE'
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source AS rst ON CASE
                WHEN trim(stg_0._source) = ''
                 OR trim(stg_0._source) IS NULL
                 OR length(trim(stg_0._source)) = 0 THEN 'A'
                ELSE trim(stg_0._source)
              END = CASE
                WHEN trim(rst.recruitment_source_desc) = ''
                 OR trim(rst.recruitment_source_desc) IS NULL
                 OR length(trim(rst.recruitment_source_desc)) = 0 THEN 'A'
                ELSE trim(rst.recruitment_source_desc)
              END
               AND upper(rst.source_system_code) = 'B'
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON coalesce(stg_0.candidate, 0) = coalesce(can.candidate_num, 0)
               AND DATE(can.valid_to_date) = '9999-12-31'
               AND upper(can.source_system_code) = 'B'
            where stg_0.candidate>-1
			GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
        ) AS stg
      QUALIFY row_number() OVER (PARTITION BY stg.candidate_profile_sid ORDER BY (stg.last_modified_date_time) DESC) = 1
    UNION ALL
    SELECT
        cast(xwlk.sk as int64) AS candidate_profile_sid,
        file_date AS valid_from_date,
        can.candidate_sid,
        CASE
           trim(stg.medium_number)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.medium_number) as INT64)
        END AS profile_medium_id,
        CAST(stg.number_ AS NUMERIC) AS candidate_profile_num,
        DATE(stg.applicationdate) AS submission_date,
		stg.applicationdate AS submission_date_time,
        DATE(stg.completeddate) AS completion_date,
		stg.completeddate AS completion_date_time,
        DATE(stg.creationdate) AS creation_date,
		stg.creationdate AS creation_date_time,
        CASE
           trim(stg.recruitmentsource_number)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.recruitmentsource_number) as INT64)
        END AS recruitment_source_id,
        CASE
           trim(stg.recruitementsourceautofilled)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.recruitementsourceautofilled) as INT64)
        END AS recruitment_source_auto_filled_sw,
        CAST(NULL as INT64) AS job_application_num,
        CAST(NULL as INT64) AS requisition_num,
        CASE
           (stg.candidate_number)
          WHEN NULL THEN 0
          ELSE CAST((stg.candidate_number) as INT64)
        END AS candidate_num,
        DATETIME('9999-12-31 23:59:59') AS valid_to_date,
        'T' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_profileinformation AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON CAST(stg.number_ as string) = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'CANDIDATE_PROFILE'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON CASE
           trim(stg.candidate_number)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.candidate_number) as INT64)
        END = can.candidate_num
         AND DATE(can.valid_to_date) = '9999-12-31'
         AND upper(can.source_system_code) = 'T'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
  ;

