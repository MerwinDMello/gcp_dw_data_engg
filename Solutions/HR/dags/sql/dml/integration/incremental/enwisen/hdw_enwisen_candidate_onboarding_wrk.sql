BEGIN
  /*  Generate the surrogate keys for Candidate_Onboarding */ 
  call
  `{{ params.param_hr_core_dataset_name }}`.sk_gen(
      '{{ params.param_hr_stage_dataset_name }}',
      'enwisen_audit',
      "employeeid||hrconumber||requisitionnumber||applicantnumber",
      'CANDIDATE_ONBOARDING'
    ); 
  call
  `{{ params.param_hr_core_dataset_name }}`.sk_gen(
    '{{ params.param_hr_stage_dataset_name }}',
    'ats_hcm_resourcetransition_stg',
    "candidate||jobrequisition||\'-ATS\'",
    "CANDIDATE_ONBOARDING"
  ); 
  /*  Truncate Work Table     */ --
TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_wrk; 
  /*  Load Work Table with working Data for Enwisen */ 
  CREATE OR REPLACE TEMPORARY TABLE can 
  -- CLUSTER BY
  -- social_security_num,
  -- requisition_num,
  -- lawson_company_num 
  AS
SELECT
  DISTINCT cp.candidate_sid,
  req.requisition_num AS requisition_num,
  req.lawson_company_num AS lawson_company_num,
  LPAD(REPLACE(replace(REPLACE(replace(REPLACE(cp.social_security_num, '-', ''),
            '.',
            ''), '/', ''),
        ' ',
        ''), '_', ''),9,'0') AS social_security_num,
  cp.valid_from_date AS cand_valid_from_date
FROM
  {{ params.param_hr_base_views_dataset_name }}.candidate_person AS cp
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.submission AS s
ON
  s.candidate_sid = cp.candidate_sid
  AND cp.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND LENGTH(REPLACE(replace(REPLACE(replace(REPLACE(cp.social_security_num, '-', ''),
            '.',
            ''), '/', ''),
        ' ',
        ''), '_', '')) < 10
  AND s.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(cp.source_system_code) = 'T'
  AND UPPER(s.source_system_code) = 'T'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr
ON
  s.recruitment_requisition_sid = rr.recruitment_requisition_sid
  AND rr.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(rr.source_system_code) = 'T'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.requisition AS req
ON
  req.requisition_sid = rr.lawson_requisition_sid
  AND req.valid_to_date = DATETIME("9999-12-31 23:59:59") ;
  /*  Load Work Table with working Data for ATS */
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_wrk (candidate_onboarding_sid,
    valid_from_date,
    requisition_sid,
    employee_sid,
    candidate_sid,
    candidate_first_name,
    candidate_last_name,
    tour_start_date,
    tour_id,
    tour_status_id,
    tour_completion_pct,
    workflow_id,
    workflow_status_id,
    email_sent_status_id,
    onboarding_confirmation_date,
    recruitment_requisition_num_text,
    process_level_code,
    applicant_num,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CAST(xwlk.sk AS INT64) AS candidate_onboarding_sid,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
  req.lawson_requisition_sid AS requisition_sid,
  NULL AS employee_sid,
  can.candidate_sid AS candidate_sid,
  TRIM(canp.first_name) AS candidate_first_name,
  TRIM(canp.last_name) AS candidate_last_name,
  DATE(stg.create_stamp_timestamp) AS tour_start_date,
  --DATE(stg.infor_lastmodified) AS tour_start_date,
  NULL AS tour_id,
  rots.tour_status_id AS tour_status_id,
  NULL AS tour_completion_pct,
  NULL AS workflow_id,
  NULL AS workflow_status_id,
  NULL AS email_sent_status_id,
--   CASE
--     WHEN STG.Status_State IN ('Complete', 'Completed') THEN STG.Completion_Date
--   ELSE
--   NULL
-- END
--   AS Onboarding_Confirmation_Date,
  DATE(sub.onboarding_confirmation_date) AS onboarding_confirmation_date,
  TRIM(req.recruitment_requisition_num_text) AS recruitment_requisition_num_text,
  TRIM(req.process_level_code) AS process_level_code,
  NULL AS applicant_num,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_hcm_resourcetransition_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  CONCAT(COALESCE(CONCAT(CAST(candidate AS STRING), CAST(jobrequisition AS STRING)), CAST(-1 AS STRING)), '-ATS') = xwlk.sk_source_txt
  AND UPPER(xwlk.sk_type) = 'CANDIDATE_ONBOARDING'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS req
ON
  stg.jobrequisition = req.requisition_num
  AND req.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(req.source_system_code) = 'B'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate AS can
ON
  stg.candidate = can.candidate_num
  AND can.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(can.source_system_code) = 'B'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate_person AS canp
ON
  can.candidate_sid = canp.candidate_sid
  AND canp.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(canp.source_system_code) = 'B'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_tour_status AS rots
ON
  TRIM(stg.status_state) = TRIM(rots.tour_status_text)
  AND UPPER(rots.source_system_code) = 'B'
LEFT OUTER JOIN (
  SELECT
    s.candidate_profile_sid,
    st.creation_date_time AS onboarding_confirmation_date,
    s.recruitment_requisition_sid,
    s.candidate_num
  FROM
    {{ params.param_hr_base_views_dataset_name }}.submission AS s
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS st
  ON
    s.candidate_profile_sid = st.candidate_profile_sid
    AND st.valid_to_date = DATETIME("9999-12-31 23:59:59")
    AND UPPER(st.source_system_code) = 'B'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status AS sts
  ON
    st.submission_tracking_sid = sts.submission_tracking_sid
    AND sts.valid_to_date = DATETIME("9999-12-31 23:59:59")
    AND UPPER(sts.source_system_code) = 'B'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.ref_submission_status AS rss
  ON
    rss.submission_status_id = sts.submission_status_id
  WHERE
    UPPER(rss.submission_status_name) = 'CONFIRMED COMPLETED'
    AND s.valid_to_date = DATETIME("9999-12-31 23:59:59")
    AND UPPER(s.source_system_code) = 'B' QUALIFY ROW_NUMBER() OVER (PARTITION BY s.candidate_profile_sid ORDER BY onboarding_confirmation_date) = 1 ) AS sub
ON
  sub.recruitment_requisition_sid = req.recruitment_requisition_sid
  AND sub.candidate_num = stg.candidate
  AND req.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(req.source_system_code) = 'B' QUALIFY ROW_NUMBER() OVER (PARTITION BY candidate_onboarding_sid ORDER BY stg.update_stamp_timestamp DESC, stg.create_stamp_timestamp DESC, sub.onboarding_confirmation_date DESC, rots.tour_status_id DESC) = 1 ;
END