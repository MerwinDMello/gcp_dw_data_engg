BEGIN
DECLARE current_ts DATETIME;
SET current_ts = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

 CALL
  {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}','taleo_sourcing_request_stg',"requisition_number||'-'|| jobboard_number",'SOURCING_REQUEST');

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.sourcing_request_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.sourcing_request_wrk (sourcing_request_sid, valid_from_date, recruitment_requisition_sid, job_board_id, source_request_status_id, job_board_type_id, valid_to_date, posting_date, unposting_date, creation_date, requisition_num, source_system_code, dw_last_update_date_time)
    SELECT DISTINCT
        cast(xwlk.sk as int64) AS sourcing_request_sid,
        current_ts AS valid_from_date,
        rr.recruitment_requisition_sid AS recruitment_requisition_sid,
        CASE
           trim(stg.jobboard_number)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.jobboard_number) as INT64)
        END AS job_board_id,
        stg.request_status_number AS source_request_status_id,
        NULL AS job_board_type_id,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        parse_date('%Y-%m-%d', substr(stg.opendate, 1, 10)) AS posting_date,
        CASE
          WHEN length(trim(stg.closedate)) = 0 THEN CAST(NULL as DATE)
          ELSE parse_date('%Y-%m-%d', substr(stg.closedate, 1, 10))
        END AS unposting_date,
        parse_date('%Y-%m-%d', substr(stg.creationdate, 1, 10)) AS creation_date,
        CASE
           trim(stg.requisition_number)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.requisition_number) as INT64)
        END AS requisition_num,
        'T' AS source_system_code,
        timestamp_trunc(current_datetime(), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_sourcing_request_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(trim(cast(stg.requisition_number as string)), '-', trim(stg.jobboard_number)) = trim(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'SOURCING_REQUEST'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr ON 
        

        CASE TRIM(stg.requisition_number)
    WHEN '' THEN 0
  ELSE
  CAST(TRIM(stg.requisition_number) AS INT64)
END
  =
  case when rr.requisition_num is null then 0.0
  
  ELSE
  CAST((rr.requisition_num) AS FLOAT64)
END
  AND UPPER(rr.source_system_code) = 'T' ;
   end     
   ;

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.sourcing_request_ats_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.sourcing_request_ats_wrk (recruitment_requisition_sid, jobrequisition, job_board_type_id, source_request_status_id, posting_date, unposting_date, creation_date)
    SELECT
        rr.recruitment_requisition_sid,
        jp.jobrequisition AS requisition_num,
        rjbt.job_board_type_id,
        sr.source_request_status_id,
        cast(jp.postingdaterangebegin as date) AS posting_date,
        cast(jp.postingdaterangeend as date) AS unposting_date,
        safe_cast(jp.createstamp as date) AS creation_date
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_hcm_jobposting_stg AS jp
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr 
        ON jp.jobrequisition = rr.requisition_num
         AND rr.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(rr.source_system_code) = 'B'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_job_board_type AS rjbt 
        ON jp.jobboard = rjbt.job_board_type_desc
         AND upper(rjbt.source_system_code) = 'B'
LEFT OUTER JOIN
         {{ params.param_hr_base_views_dataset_name }}.ref_source_request_status as sr
on coalesce(trim(case (jp.postingstatus) 
 when 1 then 'NOT POSTED'
 when 2 then 'POSTED'
 when 3 then 'EXPORTED'
 when 4 then 'SUBMITTED'
 else '' end),'xxx') = coalesce(UPPER(trim(sr.source_request_status_desc)),'XXXX')
and UPPER(TRIM(sr.source_system_code))='B'
  AND UPPER(TRIM(sr.source_system_code)) = 'B' 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY rr.recruitment_requisition_sid, rjbt.job_board_type_id, sr.source_request_status_id ORDER BY creation_date DESC) = 1 ;

CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'sourcing_request_ats_wrk',
    "recruitment_requisition_sid ||'-'||source_request_status_id || '-' || job_board_type_id || '-ATS'", 'SOURCING_REQUEST');


  INSERT INTO {{ params.param_hr_stage_dataset_name }}.sourcing_request_wrk (sourcing_request_sid, valid_from_date, recruitment_requisition_sid, job_board_id, source_request_status_id, job_board_type_id, valid_to_date, posting_date, unposting_date, creation_date, requisition_num, source_system_code, dw_last_update_date_time)
    SELECT DISTINCT
       cast(xwlk.sk as int64) AS sourcing_request_sid,
        current_datetime() AS valid_from_date,
        stg.recruitment_requisition_sid AS recruitment_requisition_sid,
        0 AS job_board_id,
        stg.source_request_status_id  AS source_request_status_id,
        stg.job_board_type_id AS job_board_type_id,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.posting_date AS posting_date,
        stg.unposting_date AS unposting_date,
        stg.creation_date AS creation_date,
        stg.jobrequisition  AS requisition_num,
        'B' AS source_system_code,
        timestamp_trunc(current_datetime(), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.sourcing_request_ats_wrk AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk 
        ON concat(stg.recruitment_requisition_sid, '-', stg.source_request_status_id, '-', stg.job_board_type_id , '-ATS') = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'SOURCING_REQUEST'
  ;
