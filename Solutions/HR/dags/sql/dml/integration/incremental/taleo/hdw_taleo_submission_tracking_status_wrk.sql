BEGIN
DECLARE current_dt DATETIME;
DECLARE DUP_COUNT INT64;
SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);  
  
  
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.submission_tracking_status_wrk;

 INSERT INTO {{ params.param_hr_stage_dataset_name }}.submission_tracking_status_wrk (file_date, submission_tracking_sid, submission_status_id, source_system_code, dw_last_update_date_time)
     SELECT
        stg.file_date,
        st.submission_tracking_sid,
        stg.status_number,
        'T' AS source_system_code,
        DATETIME_TRUNC(current_datetime(), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_applicationtrackingcswitem AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS st ON st.submission_tracking_num = stg.number
         AND st.valid_to_date = datetime("9999-12-31 23:59:59")
         AND upper(st.source_system_code) = 'T'
  ;

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.submission_tracking_status_stg;

   INSERT INTO {{ params.param_hr_stage_dataset_name }}.submission_tracking_status_stg (createstamp, candidate_profile_sid, submission_event_id, step_id, workflow_id, atsworkflowstepkey)
    SELECT
            CAST(stg.createstamp as DATETIME),
            cp.candidate_profile_sid,
            rse.submission_event_id,
            rss.step_id,
            rw.workflow_id,
            aws.atsworkflowstepkey AS atsworkflowstepkey
    FROM  {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_jobapplicationworkflowstephistory_stg AS stg
            INNER JOIN     {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp 
            ON concat(stg.jobapplication, stg.candidate, stg.jobrequisition) = concat(cp.job_application_num, cp.candidate_num, cp.requisition_num)
            AND valid_to_date = datetime("9999-12-31 23:59:59")
            AND upper(cp.source_system_code) = 'B'
            INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_event_category AS rsec 
            ON upper(trim(stg._action)) = upper(trim(rsec.submission_event_category_code))
            AND upper(rsec.source_system_code) = 'B'
            INNER JOIN         {{ params.param_hr_base_views_dataset_name }}.ref_submission_event AS rse 
            ON upper(trim(stg.triggeredby)) = upper(trim(rse.submission_event_code))
            AND rsec.submission_event_category_id = rse.submission_event_category_id
            AND upper(rse.source_system_code) = 'B'
            INNER JOIN    {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_atsworkflowstep_stg AS aws 
            ON upper(trim(concat('1/', stg.jobrequisitionworkflowstepworkflow, '/', stg.jobrequisitionworkflowstepatsworkflowstep))) = upper(aws.atsworkflowstepkey)        
            INNER JOIN     {{ params.param_hr_base_views_dataset_name }}.ref_submission_step AS rss 
            ON coalesce(trim(upper(aws.atsworkflowcategory_cube_dimension_value )), 'AA') = coalesce(trim(upper(rss.step_code)), 'AA')
            AND upper(rss.source_system_code) = 'B'
            INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_workflow AS rw 
            ON rw.workflow_code = aws.atsworkflowkey
            AND upper(rw.source_system_code) = 'B'
          QUALIFY row_number() OVER (PARTITION BY stg.createstamp, cp.candidate_profile_sid, rse.submission_event_id, rss.step_id, rw.workflow_id ORDER BY stg.updatestamp DESC) = 1
      ;


   INSERT INTO {{ params.param_hr_stage_dataset_name }}.submission_tracking_status_wrk (file_date, submission_tracking_sid, submission_status_id, source_system_code, dw_last_update_date_time)
     SELECT
        current_date() AS file_date,
       cast( xwlk.sk as int64) AS submission_tracking_sid ,
        cast(rs.submission_status_id as string) AS submission_status_id,
        'B' AS source_system_code,
        DATETIME_TRUNC(current_datetime(), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.submission_tracking_status_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(trim(cast(stg.createstamp as string)), '-', trim(cast(stg.candidate_profile_sid as string)), '-', trim(cast(stg.submission_event_id as string)), '-', trim(cast(stg.step_id as string)), '-', trim(cast(stg.workflow_id as string))) = upper(xwlk.sk_source_txt)
         AND upper(TRIM(xwlk.sk_type)) = 'SUBMISSION_TRACKING'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_status AS rs ON stg.atsworkflowstepkey = rs.submission_status_code
         AND upper(TRIM(rs.source_system_code))= 'B'
  ;
end