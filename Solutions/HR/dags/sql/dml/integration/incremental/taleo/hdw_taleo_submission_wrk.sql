  /*  Generate the surrogate keys for Submission */
CALL
 {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}','taleo_application','cast(number as string)','SUBMISSION');
  /*  Truncate Worktable Table     */
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.submission_wrk; /*  Load Work Table with working Data */
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.submission_wrk (submission_sid,
    valid_from_date,
    valid_to_date,
    submission_num,
    last_modified_date,
    new_submission_sw,
    candidate_sid,
    recruitment_requisition_sid,
    candidate_profile_sid,
    current_submission_status_id,
    current_submission_step_id,
    current_submission_workflow_id,
    requisition_num,
    job_application_num,
    candidate_num,
    matched_from_requisition_num,
    matched_candidate_flag,
    submission_source_code,
    source_system_code,
    dw_last_update_date_time)
SELECT
  cast(xwlk.sk as int64) AS submission_sid,
  stg.file_date AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  (stg.number) AS submission_num,
  cast(stg.lastmodifieddate as date) AS last_modified_date,
  CASE TRIM(stg.newapplication)
    WHEN '' THEN 0
  ELSE
  CAST(TRIM(stg.newapplication) AS INT64)
END
  AS new_submission_sw,
  can.candidate_sid,
  rec_rq.recruitment_requisition_sid,
  can_pf.candidate_profile_sid,
  CASE TRIM(stg.cswlateststatus_number)
    WHEN '' THEN 0
  ELSE
  CAST(TRIM(stg.cswlateststatus_number) AS INT64)
END
  AS current_submission_status_id,
  CASE TRIM(stg.cswlateststep_number)
    WHEN '' THEN 0
  ELSE
  CAST(TRIM(stg.cswlateststep_number) AS INT64)
END
  AS current_submission_step_id,
  CASE TRIM(stg.cswworkflow_number)
    WHEN '' THEN 0
  ELSE
  CAST(TRIM(stg.cswworkflow_number) AS INT64)
END
  AS current_submission_workflow_id,
  CASE TRIM(stg.requisition_number)
    WHEN '' THEN 0
  ELSE
  CAST(TRIM(stg.requisition_number) AS INT64)
END
  AS requisition_num,
  CAST(NULL AS INT64) AS job_application_num,
  CASE TRIM(stg.candidate_number)
    WHEN '' THEN 0
  ELSE
  CAST(TRIM(stg.candidate_number) AS INT64)
END
  AS candidate_num,
  tr.requisition_number AS matched_from_requisition_num,
 CASE WHEN rpm.profile_medium_desc = 'Matched to Job' THEN 'Y' 
	ELSE 'N' END AS matched_candidate_flag,
  cast(NULL as string) AS submission_source_code,
  'T' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.taleo_application AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  --UPPER(SUBSTR((stg.number), 1, 255)) = UPPER(xwlk.sk_source_txt)
  (SUBSTR(cast(stg.number as string), 1, 255)) = (xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'SUBMISSION'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate AS can
ON
  CASE TRIM(stg.candidate_number)
    WHEN '' THEN 0
  ELSE
  CAST(TRIM(stg.candidate_number) AS INT64)
END
  = can.candidate_num
  AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(can.source_system_code) = 'T'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rec_rq
ON
  CASE TRIM(stg.requisition_number)
    WHEN '' THEN 0
  ELSE
  CAST(TRIM(stg.requisition_number) AS INT64)
END
  = rec_rq.requisition_num
  AND (rec_rq.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(rec_rq.source_system_code) = 'T'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS can_pf
ON
  CASE TRIM(stg.profileinformation_number)
    WHEN '' THEN 0
  ELSE
  CAST(TRIM(stg.profileinformation_number) AS INT64)
END
  = can_pf.candidate_profile_num
  AND (can_pf.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(can_pf.source_system_code) = 'T'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_profile_medium rpm
ON can_pf.profile_medium_id = rpm.profile_medium_id
AND rpm.source_system_code='T'
LEFT OUTER JOIN
  {{ params.param_hr_stage_dataset_name }}.taleo_matched_requisition AS tr
ON
  tr.app_number = stg.number
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13,
  14,
  15,
  16,
  17 ;

  -- -Added ATS code as per HDM-1423
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.submission_wrk (submission_sid,
    valid_from_date,
    valid_to_date,
    submission_num,
    last_modified_date,
    new_submission_sw,
    candidate_sid,
    recruitment_requisition_sid,
    candidate_profile_sid,
    current_submission_status_id,
    current_submission_step_id,
    current_submission_workflow_id,
    requisition_num,
    job_application_num,
    candidate_num,
    matched_from_requisition_num,
    matched_candidate_flag,
    submission_source_code,
    source_system_code,
    dw_last_update_date_time)
SELECT
  cp.candidate_profile_sid AS submission_sid,
  CURRENT_DATE() AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  cp.candidate_profile_num AS submission_num,
  DATE(stg._asoftimestamp) AS last_modified_date,
  CASE
    WHEN stg.selectionprocess = 2 THEN 1
  ELSE
  0
END
  AS new_submission_sw,
  can.candidate_sid AS candidate_sid,
  rr.recruitment_requisition_sid AS recruitment_requisition_sid,
  cp.candidate_profile_sid AS candidate_profile_sid,
  rss.submission_status_id AS current_submission_status_id,
  rsst.step_id AS current_submission_step_id,
  rw.workflow_id AS current_submission_workflow_id,
  stg.jobrequisition AS requisition_num,
  stg.jobapplication AS job_application_num,
  stg.candidate AS candidate_num,
  stg.hcajobapplicationmatchedrequisition AS matched_from_requisition_num,
  CASE WHEN stg.attachedbyrecruiter  = 1 OR stg.appliedbyrecruiter = 1 OR stg.hcajobapplicationmatchedrequisition > 1 THEN 'Y' 
   ELSE 'N' END AS matched_candidate_flag,
  stg.specificsource AS submission_source_code,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp
ON
  stg.jobapplication = cp.job_application_num
  AND stg.candidate= cp.candidate_num
  AND stg.jobrequisition = cp.requisition_num
  AND (cp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(cp.source_system_code) = 'B'
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_atsworkflowstep_stg AS ws
ON
  COALESCE(UPPER(TRIM(stg.atsworkflowstepkey)),'') = COALESCE(UPPER(TRIM(ws.atsworkflowstepkey)),'')
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_submission_status AS rss
ON
  COALESCE(UPPER(TRIM(ws.atsworkflowstepkey)),'') = COALESCE(UPPER(TRIM(rss.submission_status_code)),'')
  AND UPPER(rss.source_system_code) = 'B'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_submission_step AS rsst
ON
  COALESCE(UPPER(TRIM(ws.atsworkflowcategory_cube_dimension_value)),'') = COALESCE(UPPER(TRIM(rsst.step_name)),'')
  AND UPPER(rsst.source_system_code) = 'B'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_workflow AS rw
ON
  COALESCE(UPPER(TRIM(ws.atsworkflowkey)),'') = COALESCE(UPPER(TRIM(rw.workflow_code)),'')
  AND UPPER(rw.source_system_code) = 'B'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate AS can
ON
  stg.candidate = can.candidate_num
  AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(can.source_system_code) = 'B'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr
ON
  stg.jobrequisition = rr.requisition_num
  AND (rr.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(rr.source_system_code) = 'B' QUALIFY ROW_NUMBER() OVER (PARTITION BY job_application_num, candidate_num, requisition_num ORDER BY stg.createstamp DESC) = 1 ;