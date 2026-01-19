BEGIN
DECLARE   current_ts DATETIME;
DECLARE   DUP_COUNT INT64;
 
SET
  current_ts = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);


  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.recruitment_job_board_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.recruitment_job_board_wrk (file_date, recruitment_job_sid, job_board_id, posting_board_type_id, posting_status_id, posting_date, unposting_date, source_system_code, dw_last_update_date_time)
    SELECT
        stg.file_date,
        rj.recruitment_job_sid,
        jb.job_board_id,
        pbt.posting_board_type_id,
        ps.posting_status_id,
        parse_date('%Y-%m-%d', substr(stg.postingdate, 1, strpos(stg.postingdate, ' ') - 1)) AS posting_date,
        parse_date('%Y-%m-%d', substr(stg.unpostingdate, 1, strpos(stg.unpostingdate, ' ') - 1)) AS unposting_date,
        'T' AS source_system_code,
        timestamp_trunc(current_DATETIME(), SECOND) AS dw_last_update_date_time
--TIMESTAMP_TRUNC(CURRENT_

      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_jobinformation AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj ON stg.number = rj.recruitment_job_num
         AND upper(rj.source_system_code) = 'T'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_job_board AS jb ON coalesce(trim(stg.jobboardid), 'XXX') = coalesce(trim(cast(jb.job_board_id as string)),'XXX')
         AND upper(jb.source_system_code) = 'T'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_posting_board_type AS pbt ON coalesce(trim(stg.postingboardtype), 'XXX') = coalesce(trim(pbt.posting_board_type_code), 'XXX')
         AND upper(pbt.source_system_code) = 'T'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_posting_status AS ps ON coalesce(trim(stg.postingstatus), 'XXX') = coalesce(trim(ps.posting_status_code), 'XXX')
         AND upper(ps.source_system_code) = 'T'
      WHERE stg.number <> 0
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
  ;

BEGIN
 
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.recruitment_job_board_wrk (file_date, recruitment_job_sid, job_board_id, posting_board_type_id, posting_status_id, posting_date, unposting_date, source_system_code, dw_last_update_date_time)
    SELECT
        x.file_date,
        x.recruitment_job_sid,
        x.job_board_id,
        x.posting_board_type_id,
        x.posting_status_id,
        x.posting_date,
        x.unposting_date,
        x.source_system_code,
        x.dw_last_update_date_time
      FROM
        (
          SELECT
              row_number() OVER (ORDER BY jp.postingdaterangebegin) AS row_number_id,
              current_date() AS file_date,
              rj.recruitment_job_sid,
              jb.job_board_id,
              pbt.posting_board_type_id,
              ps.posting_status_id,
              cast(jp.postingdaterangebegin as date) AS posting_date,
              cast(jp.postingdaterangeend as date) AS unposting_date,
              'B' AS source_system_code,
              timestamp_trunc(current_DATETIME(), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS stg
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj ON stg.jobrequisition = rj.recruitment_job_num
               AND upper(rj.source_system_code) = 'B'
              INNER JOIN {{ params.param_hr_stage_dataset_name }}.ats_hcm_jobposting_stg AS jp ON stg.jobrequisition = jp.jobrequisition
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_job_board AS jb ON coalesce(trim(jp.jobboard), 'XXX') = coalesce(trim(cast(jb.job_board_id as string)), 'XXX')
               AND upper(jb.source_system_code) = 'B'
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_posting_board_type AS pbt ON coalesce(trim(jp.jobboard), 'XXX') = coalesce(trim(pbt.posting_board_type_code), 'XXX')
               AND upper(pbt.source_system_code) = 'B'
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_posting_status AS ps ON coalesce(trim(CASE
                WHEN CASE
                   trim(cast(jp.postingstatus as string))
                  WHEN '' THEN 0.0
                  ELSE CAST(trim(cast(jp.postingstatus as string)) as FLOAT64)
                END = 1 THEN 'Not Posted'
                WHEN CASE
                   trim(cast(jp.postingstatus as string))
                  WHEN '' THEN 0.0
                  ELSE CAST(trim(cast(jp.postingstatus as string)) as FLOAT64)
                END = 2 THEN 'Posted'
                WHEN CASE
                   trim(cast(jp.postingstatus as string))
                  WHEN '' THEN 0.0
                  ELSE CAST(trim(cast(jp.postingstatus as string)) as FLOAT64)
                END = 3 THEN 'Exported'
                WHEN CASE
                   trim(cast(jp.postingstatus as string))
                  WHEN '' THEN 0.0
                  ELSE CAST(trim(cast(jp.postingstatus as string)) as FLOAT64)
                END = 4 THEN 'Submitted'
                ELSE ''
              END), 'XXX') = coalesce(trim(ps.posting_status_code), 'XXX')
               AND upper(ps.source_system_code) = 'B'
            ---GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10
            QUALIFY row_number() OVER (PARTITION BY jp.postingdaterangebegin ORDER BY jp.postingdaterangebegin DESC) = 1
        ) AS x
;
END;
END
  ;

