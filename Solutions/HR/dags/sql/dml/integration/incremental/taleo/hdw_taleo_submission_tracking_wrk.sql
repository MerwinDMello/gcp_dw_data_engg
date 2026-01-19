--hdw_taleo_submission_tracking_wrk.sql


  
  

  
  


  

/* Generate the SK */
CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'taleo_applicationtrackingcswitem', 'CAST(Number AS STRING)', 'Submission_Tracking');
/*  Truncate Worktable Table */

  
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.submission_tracking_wrk;

  
  

/*  Load Work Table with working Data */

  


  
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.submission_tracking_wrk (file_date, submission_tracking_sid, candidate_profile_sid, submission_tracking_num, creation_date_time, event_date_time, event_detail_text, submission_event_id, tracking_user_sid, tracking_step_id, tracking_workflow_id, sub_status_desc, step_reverted_ind, source_system_code, dw_last_update_date_time)
    SELECT
        stg.file_date AS file_date,
        cast(xwlk.sk as int64) AS submission_tracking_sid,
        sp.candidate_profile_sid AS candidate_profile_sid,
        stg.number AS submission_tracking_num,
        stg.creationdate AS creation_date_time,
        stg.eventdate AS event_date_time,
        stg.detail AS event_detail_text,
        stg.event_number AS submission_event_id,
        ru.recruitment_user_sid AS tracking_user_sid,
        stg.step_number AS tracking_step_id,
        stg.workflow_number AS tracking_workflow_id,
        NULL AS sub_status_desc,
        CASE
          WHEN trim(stg.reverted) = 'false' THEN 'N'
          WHEN trim(stg.reverted) = 'true' THEN 'Y'
          ELSE trim(stg.reverted)
        END AS step_reverted_ind,
        stg.source_system_code AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_applicationtrackingcswitem AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk 
        ON
         (SUBSTR(cast(stg.number as string), 1, 255)) = (xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'SUBMISSION_TRACKING'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS sp ON stg.profileinformation_number = sp.candidate_profile_num
         AND (sp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS ru ON stg.user_userno = ru.recruitment_user_num
         AND (ru.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  ;


  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.submission_tracking_stg;


  INSERT INTO {{ params.param_hr_stage_dataset_name }}.submission_tracking_stg (createstamp, candidate_profile_sid, submission_event_id, step_id, workflow_id,sub_status_desc,
  --TDGCP-2862
  moved_by_text
  --TDGCP-2862
  )
    SELECT
        CAST(stg.createstamp as DATETIME),
        cp.candidate_profile_sid,
        rse.submission_event_id,
        rss.step_id,
        rw.workflow_id,
        stg.hcajobapplicationworkflowstephistorysubstatus,
		--TDGCP-2862
		CAST(stg.movedby AS STRING)
		--TDGCP-2862
      FROM {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_jobapplicationworkflowstephistory_stg  AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp ON concat(stg.jobapplication, stg.candidate, stg.jobrequisition) = concat(cp.job_application_num, cp.candidate_num, cp.requisition_num)
         AND (valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(cp.source_system_code) = 'B'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_event_category AS rsec         
        ON upper(trim(stg._action)) = upper(trim(rsec.submission_event_category_code))
         AND upper(rsec.source_system_code) = 'B'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_event AS rse 
        ON upper(trim(stg.triggeredby)) = upper(trim(rse.submission_event_code))
         AND rsec.submission_event_category_id = rse.submission_event_category_id
         AND upper(rse.source_system_code) = 'B'
		 
        INNER JOIN    {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_atsworkflowstep_stg AS aws 
        ON upper(trim(concat('1/', stg.jobrequisitionworkflowstepworkflow, '/', stg.jobrequisitionworkflowstepatsworkflowstep))) = upper(aws.atsworkflowstepkey)  /*      
        INNER JOIN  {{ params.param_hr_base_views_dataset_name }}.ref_submission_step AS rss 
        ON coalesce(trim(upper(aws.atsworkflowcategory_cube_dimension_value )), 'AA') = coalesce(trim(upper(rss.step_code)), 'AA')
        AND upper(rss.source_system_code) = 'B'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_workflow AS rw 
        ON rw.workflow_code = aws.atsworkflowkey
         AND upper(rw.source_system_code) = 'B'
		 */
		 INNER JOIN  {{ params.param_hr_base_views_dataset_name }}.ref_submission_step  RSS
        ON CASE WHEN (RSS.step_code IS NULL OR Char_Length(Trim(RSS.step_code))=0) THEN 'AA' ELSE Trim(RSS.step_code) END =
        CASE WHEN (STG.atsworkflowcategory IS NULL OR Char_Length(Trim(STG.atsworkflowcategory))=0)
        THEN 'AA' ELSE Trim('1/'||STG.jobrequisitionworkflow|| '/' ||STG.atsworkflowcategory) END
        AND upper(RSS.source_system_code)='B'
		INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_workflow RW 
        ON RW.workflow_code = '1/'||STG.jobrequisitionworkflow -- Changed from AWS.ATSWORKFLOW_CDV to '1/'||STG.ATSWORKFLOW
        AND upper(RW.source_system_code)='B'

		GROUP BY 1,2,3,4,5,6,7
--QUALIFY row_number() OVER (PARTITION BY stg.createstamp, cp.candidate_profile_sid, rse.submission_event_id, rss.step_id, rw.workflow_id ORDER BY stg.updatestamp DESC) = 1
  ;
  
     -- TDGCP-2862




  
  

--  Changed from AWS.ATSWORKFLOW_CDV to '1/'||STG.ATSWORKFLOW
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY STG.UpdateStamp,CP.CANDIDATE_PROFILE_SID,RSE.SUBMISSION_EVENT_ID,RSS.STEP_ID,RW.WORKFLOW_ID  ORDER BY STG.HCAStepHistSubStatus) = 1;

  

CALL {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'submission_tracking_stg', 'TRIM(COALESCE(CAST(CreateStamp AS STRING),\'\')) ||\'-\'|| TRIM(COALESCE(CAST(Candidate_Profile_SID AS STRING),\'\')) ||\'-\'|| TRIM(COALESCE(CAST(Submission_Event_Id AS STRING),\'\'))||\'-\'||TRIM(COALESCE(CAST(Step_Id AS STRING),\'\')) ||\'-\'|| TRIM(COALESCE(CAST(Workflow_Id AS STRING),\'\'))', 'Submission_Tracking');

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.submission_tracking_wrk (file_date,
    submission_tracking_sid,
    candidate_profile_sid,
    submission_tracking_num,
    creation_date_time,
    event_date_time,
    event_detail_text,
    submission_event_id,
    tracking_user_sid,
    tracking_step_id,
    tracking_workflow_id,
    sub_status_desc,
	--TDGCP-2862
    moved_by_text,
    --TDGCP-2862
    step_reverted_ind,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE() AS file_date,
  cast(xwlk.sk as int64) AS submission_tracking_sid,
  stg.candidate_profile_sid,
  cast(xwlk.sk as int64) AS submission_tracking_num,
  CAST(SUBSTR(CAST(stg.createstamp AS STRING), 1, 19) AS datetime) AS creation_date_time,
  NULL AS event_date_time,
  NULL AS event_detail_text,
  stg.submission_event_id,
  0 AS tracking_user_sid,
  stg.step_id AS tracking_step_id,
  stg.workflow_id AS tracking_workflow_id,
  stg.sub_status_desc,
  --TDGCP-2862
  trim(stg.moved_by_text),
  --TDGCP-2862
  'N' AS step_reverted_ind,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.submission_tracking_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON

upper(concat(trim
(coalesce(cast(stg.createstamp as string), '\'')), '-', trim(coalesce(cast(stg.candidate_profile_sid as string), '\'')), '-', trim(coalesce(cast(stg.submission_event_id as string), '\'')), '-', trim(coalesce(cast(stg.step_id as string), '\'')), '-', trim(coalesce(cast(stg.workflow_id as string), '\'')))
) = upper(xwlk.sk_source_txt) 
  AND UPPER(TRIM(xwlk.sk_type)) = 'SUBMISSION_TRACKING' ;