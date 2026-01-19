TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_wrk;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_wrk (requisition_number,
    process_level_code,
    data_type,
    dw_last_update_date_time)
SELECT
  iq.requisition_number,
  iq.process_level_code,
  iq.data_type,
  iq.dw_last_update_date_time
FROM (
  SELECT
    DISTINCT TRIM(REVERSE(TRIM(SUBSTR(REVERSE(enwisen_cm5tickets_stg.subject), 1, STRPOS(REVERSE(enwisen_cm5tickets_stg.subject), '-') - 1)))) AS requisition_number,
    TRIM(REVERSE(TRIM(SUBSTR(REVERSE(enwisen_cm5tickets_stg.subject), STRPOS(REVERSE(enwisen_cm5tickets_stg.subject), '-') + 1, 5)))) AS process_level_code,
    'T' AS data_type,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
  FROM
    {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg
  WHERE
    UPPER(enwisen_cm5tickets_stg.topic) = 'Z-AUTO-ONBOARDING CONFIRM'
    AND UPPER(enwisen_cm5tickets_stg.category) = 'Z-AUTO-ONBOARDING CONFIRM'
    AND UPPER(enwisen_cm5tickets_stg.subcategory) = 'Z-AUTO-ONBOARDING CONFIRM'
    AND STRPOS(enwisen_cm5tickets_stg.subject, '-') <> 0
    AND UPPER(enwisen_cm5tickets_stg.subject) LIKE 'ONBOARDING ACTION REQUIRED %'
    AND UPPER(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ -%'
    AND UPPER(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ-%'
    AND UPPER(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ %'
    AND UPPER(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQUISITION%'
  UNION DISTINCT
  SELECT
    DISTINCT TRIM(REVERSE(TRIM(SUBSTR(REVERSE(enwisen_cm5tickets_stg_0.subject), 1, STRPOS(REVERSE(enwisen_cm5tickets_stg_0.subject), '-') - 1)))) AS requisition_number,
    TRIM(REVERSE(TRIM(SUBSTR(REVERSE(enwisen_cm5tickets_stg_0.subject), STRPOS(REVERSE(enwisen_cm5tickets_stg_0.subject), '-') + 1, 5)))) AS process_level_code,
    'D' AS data_type,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
  FROM
    {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS enwisen_cm5tickets_stg_0
  WHERE
    UPPER(enwisen_cm5tickets_stg_0.topic) = 'HIRE MY EMPLOYEES'
    AND UPPER(enwisen_cm5tickets_stg_0.category) = 'HIRING EMPLOYEES'
    AND UPPER(enwisen_cm5tickets_stg_0.subcategory) = 'DRUG SCREEN RESULTS'
    AND UPPER(enwisen_cm5tickets_stg_0.subject) LIKE 'CONFIRMED DS RESULTS%'
    AND UPPER(enwisen_cm5tickets_stg_0.subject) NOT LIKE '%ACQ -%'
    AND UPPER(enwisen_cm5tickets_stg_0.subject) NOT LIKE '%ACQ-%'
    AND UPPER(enwisen_cm5tickets_stg_0.subject) NOT LIKE '%ACQ %'
    AND UPPER(enwisen_cm5tickets_stg_0.subject) NOT LIKE '%ACQUISITION%'
  UNION DISTINCT
  SELECT
    DISTINCT TRIM(REVERSE(TRIM(SUBSTR(REPLACE(REVERSE(enwisen_cm5tickets_stg_1.subject), ')', ''), 1, STRPOS(REPLACE(REVERSE(enwisen_cm5tickets_stg_1.subject), ')', ''), '-') - 1)))) AS requisition_number,
    TRIM(REVERSE(TRIM(SUBSTR(REVERSE(enwisen_cm5tickets_stg_1.subject), STRPOS(REVERSE(enwisen_cm5tickets_stg_1.subject), '-') + 1, 5)))) AS process_level_code,
    'B' AS data_type,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
  FROM
    {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS enwisen_cm5tickets_stg_1
  WHERE
    UPPER(enwisen_cm5tickets_stg_1.topic) = 'Z-AUTO-BG CHECK AUTH'
    AND UPPER(enwisen_cm5tickets_stg_1.category) = 'Z-AUTO-BG CHECK AUTH'
    AND UPPER(enwisen_cm5tickets_stg_1.subcategory) = 'Z-AUTO-BG CHECK AUTH'
    AND UPPER(enwisen_cm5tickets_stg_1.subject) LIKE 'BG AUTH COMPLETE%'
    AND UPPER(enwisen_cm5tickets_stg_1.subject) NOT LIKE '%ACQ -%'
    AND UPPER(enwisen_cm5tickets_stg_1.subject) NOT LIKE '%ACQ-%'
    AND UPPER(enwisen_cm5tickets_stg_1.subject) NOT LIKE '%ACQ %'
    AND UPPER(enwisen_cm5tickets_stg_1.subject) NOT LIKE '%ACQUISITION%'
    AND STRPOS(enwisen_cm5tickets_stg_1.subject, '-') <> 0 ) AS iq
WHERE
  LENGTH(iq.requisition_number) <> 0
  AND LENGTH(iq.process_level_code) <> 0
  AND LOWER(iq.requisition_number) = UPPER(iq.requisition_number)
  AND LOWER(iq.process_level_code) = UPPER(iq.process_level_code)
  AND iq.requisition_number NOT LIKE '%*%'
  AND iq.requisition_number NOT LIKE '%/%'
  AND iq.requisition_number NOT LIKE '%)%'
  AND iq.requisition_number NOT LIKE '%(%'
  AND iq.requisition_number NOT LIKE '%.%'
  AND iq.requisition_number NOT LIKE '%]%'
  AND iq.requisition_number NOT LIKE '%[%'
  AND iq.requisition_number NOT LIKE '% %'
  AND iq.requisition_number NOT LIKE '%:%'
  AND iq.requisition_number NOT LIKE '%\'%' ;
call
  `{{ params.param_hr_core_dataset_name }}`.sk_gen(
    '{{ params.param_hr_stage_dataset_name }}',
    'candidate_onboarding_event_xwlk_wrk',
    "requisition_number||process_level_code||data_type",
    'CANDIDATE_ONBOARDING_EVENT');
/*  Truncate Work Table     */
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_wrk; /*  Load Work Table with working Data */
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_wrk (candidate_onboarding_event_sid,
    valid_from_date,
    event_type_id,
    recruitment_requisition_num_text,
    completed_date,
    candidate_sid,
    resource_screening_package_num,
    sequence_num,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CAST(xwlk.sk AS INT64) AS candidate_onboarding_event_sid,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
  CAST(roe.event_type_id AS STRING) AS event_type_id,
  CONCAT(TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg.subject), STRPOS(REVERSE(stg.subject), '-') + 1, 5)))), '-', TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg.subject), 1, STRPOS(REVERSE(stg.subject), '-') - 1))))) AS recruitment_requisition_num_text,
  CAST(TRIM(stg.createddatetime) AS DATETIME) AS completed_date,
  CAST(NULL AS INT64) AS candidate_sid,
  CAST(NULL AS INT64) AS resource_screening_package_num,
  CAST(NULL AS INT64) AS sequence_num,
  'W' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  COALESCE(CONCAT(TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg.subject), 1, STRPOS(REVERSE(stg.subject), '-') - 1)))), TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg.subject), STRPOS(REVERSE(stg.subject), '-') + 1, 5)))), 'T'), CAST(-1 AS STRING)) = xwlk.sk_source_txt
  AND UPPER(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type AS roe
ON
  UPPER(event_type_code) = 'T'
WHERE
  UPPER(stg.topic) = 'Z-AUTO-ONBOARDING CONFIRM'
  AND UPPER(stg.category) = 'Z-AUTO-ONBOARDING CONFIRM'
  AND UPPER(stg.subcategory) = 'Z-AUTO-ONBOARDING CONFIRM'
  AND STRPOS(stg.subject, '-') <> 0
  AND UPPER(stg.subject) LIKE 'ONBOARDING ACTION REQUIRED %'
  AND UPPER(stg.subject) NOT LIKE '%ACQ -%'
  AND UPPER(stg.subject) NOT LIKE '%ACQ-%'
  AND UPPER(stg.subject) NOT LIKE '%ACQ %'
  AND UPPER(stg.subject) NOT LIKE '%ACQUISITION%' QUALIFY ROW_NUMBER() OVER (PARTITION BY candidate_onboarding_event_sid ORDER BY stg.createddatetime DESC) = 1
UNION DISTINCT
SELECT
  CAST(xwlk_0.sk AS INT64) AS candidate_onboarding_event_sid,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
  CAST(roe_0.event_type_id AS STRING) AS event_type_id,
  CONCAT(TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg_0.subject), STRPOS(REVERSE(stg_0.subject), '-') + 1, 5)))), '-', TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg_0.subject), 1, STRPOS(REVERSE(stg_0.subject), '-') - 1))))) AS recruitment_requisition_num_text,
  CAST(TRIM(stg_0.createddatetime) AS DATETIME) AS completed_date,
  CAST(NULL AS INT64) AS candidate_sid,
  CAST(NULL AS INT64) AS resource_screening_package_num,
  CAST(NULL AS INT64) AS sequence_num,
  'W' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS stg_0
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk_0
ON
  COALESCE(CONCAT(TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg_0.subject), 1, STRPOS(REVERSE(stg_0.subject), '-') - 1)))), TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg_0.subject), STRPOS(REVERSE(stg_0.subject), '-') + 1, 5)))), 'D'), CAST(-1 AS STRING)) = xwlk_0.sk_source_txt
  AND UPPER(xwlk_0.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type AS roe_0
ON
  UPPER(event_type_code) = 'D'
WHERE
  UPPER(stg_0.topic) = 'HIRE MY EMPLOYEES'
  AND UPPER(stg_0.category) = 'HIRING EMPLOYEES'
  AND UPPER(stg_0.subcategory) = 'DRUG SCREEN RESULTS'
  AND UPPER(stg_0.subject) LIKE 'CONFIRMED DS RESULTS%'
  AND UPPER(stg_0.subject) NOT LIKE '%ACQ -%'
  AND UPPER(stg_0.subject) NOT LIKE '%ACQ-%'
  AND UPPER(stg_0.subject) NOT LIKE '%ACQ %'
  AND UPPER(stg_0.subject) NOT LIKE '%ACQUISITION%' QUALIFY ROW_NUMBER() OVER (PARTITION BY candidate_onboarding_event_sid ORDER BY stg_0.createddatetime DESC) = 1
UNION DISTINCT
SELECT
  CAST(xwlk_1.sk AS INT64) AS candidate_onboarding_event_sid,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
  CAST(roe_1.event_type_id AS STRING) AS event_type_id,
  CONCAT(TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg_1.subject), STRPOS(REVERSE(stg_1.subject), '-') + 1, 5)))), '-', TRIM(REVERSE(TRIM(SUBSTR(REPLACE(REVERSE(stg_1.subject), ')', ''), 1, STRPOS(REPLACE(REVERSE(stg_1.subject), ')', ''), '-') - 1))))) AS recruitment_requisition_num_text,
  CAST(TRIM(stg_1.createddatetime) AS DATETIME) AS completed_date,
  CAST(NULL AS INT64) AS candidate_sid,
  CAST(NULL AS INT64) AS resource_screening_package_num,
  CAST(NULL AS INT64) AS sequence_num,
  'W' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS stg_1
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk_1
ON
  COALESCE(CONCAT(TRIM(REVERSE(TRIM(SUBSTR(REPLACE(REVERSE(stg_1.subject), ')', ''), 1, STRPOS(REPLACE(REVERSE(stg_1.subject), ')', ''), '-') - 1)))), TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg_1.subject), STRPOS(REVERSE(stg_1.subject), '-') + 1, 5)))), 'B'), CAST(-1 AS STRING)) = xwlk_1.sk_source_txt
  AND UPPER(xwlk_1.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type AS roe_1
ON
  UPPER(event_type_code) = 'B'
WHERE
  UPPER(stg_1.topic) = 'Z-AUTO-BG CHECK AUTH'
  AND UPPER(stg_1.category) = 'Z-AUTO-BG CHECK AUTH'
  AND UPPER(stg_1.subcategory) = 'Z-AUTO-BG CHECK AUTH'
  AND UPPER(stg_1.subject) LIKE 'BG AUTH COMPLETE%'
  AND UPPER(stg_1.subject) NOT LIKE '%ACQ -%'
  AND UPPER(stg_1.subject) NOT LIKE '%ACQ-%'
  AND UPPER(stg_1.subject) NOT LIKE '%ACQ %'
  AND UPPER(stg_1.subject) NOT LIKE '%ACQUISITION%'
  AND STRPOS(stg_1.subject, '-') <> 0 QUALIFY ROW_NUMBER() OVER (PARTITION BY candidate_onboarding_event_sid ORDER BY stg_1.createddatetime DESC) = 1 ;
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_ats;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_ats (jobrequisition,
    candidate,
    event_type_id,
    dw_last_update_date_time)
SELECT
  MAX(CAST(ats_hcm_resourcetransition_stg.jobrequisition AS STRING)) AS jobrequisition,
  MAX(CAST(ats_hcm_resourcetransition_stg.candidate AS STRING)) AS candidate,
  '2' AS event_type_id,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_hcm_resourcetransition_stg
WHERE
  UPPER(ats_hcm_resourcetransition_stg.status_state) = 'COMPLETE'
GROUP BY
  ats_hcm_resourcetransition_stg.jobrequisition,
  ats_hcm_resourcetransition_stg.candidate,
  4
UNION DISTINCT
SELECT
  MAX(CAST(ats_cust_resourcescreening_stg.resourcescreeningpackage AS STRING)) AS jobrequisition,
  MAX(CAST(ats_cust_resourcescreening_stg.sequencenumber AS STRING)) AS candidate,
  '1' AS event_type_id,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_resourcescreening_stg
WHERE
  UPPER(ats_cust_resourcescreening_stg.hcaresourcescreeningvendorstatus) = 'COMPLETED'
  AND UPPER(ats_cust_resourcescreening_stg.screening) = 'DRUGSCREEN'
GROUP BY
  ats_cust_resourcescreening_stg.resourcescreeningpackage,
  ats_cust_resourcescreening_stg.sequencenumber,
  4 ; 
call
  `{{ params.param_hr_core_dataset_name }}`.sk_gen(
    '{{ params.param_hr_stage_dataset_name }}',
    'candidate_onboarding_event_xwlk_ats',
    'jobrequisition||candidate||event_type_id||\'-B\'',
    'CANDIDATE_ONBOARDING_EVENT');
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_stg;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_stg (requisition_num,
    candidate_num,
    event_type_id,
    recruitment_requisition_num_text,
    creation_date_time,
    candidate_sid,
    dw_last_update_date_time)
SELECT
  CAST(MAX(s.requisition_num) AS STRING) AS requisition_num,
  CAST(MAX(s.candidate_num) AS STRING) AS candidate_num,
  '3' AS event_type_id,
  rr.recruitment_requisition_num_text,
  st.creation_date_time,
  s.candidate_sid,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS st
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.submission_tracking_status AS sts
ON
  st.submission_tracking_sid = sts.submission_tracking_sid
  AND sts.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(sts.source_system_code) = 'B'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_submission_status AS rss
ON
  sts.submission_status_id = rss.submission_status_id
  AND UPPER(rss.source_system_code) = 'B'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp
ON
  st.candidate_profile_sid = cp.candidate_profile_sid
  AND cp.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(cp.source_system_code) = 'B'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.submission AS s
ON
  cp.candidate_profile_sid = s.candidate_profile_sid
  AND s.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(s.source_system_code) = 'B'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr
ON
  s.recruitment_requisition_sid = rr.recruitment_requisition_sid
  AND rr.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(rr.source_system_code) = 'B'
WHERE
  st.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(st.source_system_code) = 'B'
  AND UPPER(rss.submission_status_desc) = 'CANDIDATE CONSENT PENDING'
GROUP BY
  s.requisition_num,
  s.candidate_num,
  4,
  5,
  6,
  7 ;
call 
  `{{ params.param_hr_core_dataset_name }}`.sk_gen(
  '{{ params.param_hr_stage_dataset_name }}',
  'candidate_onboarding_event_xwlk_stg',
  'requisition_num||candidate_num||event_type_id||\'-B\'',
  'CANDIDATE_ONBOARDING_EVENT'
); 
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_wrk (candidate_onboarding_event_sid,
    valid_from_date,
    event_type_id,
    recruitment_requisition_num_text,
    completed_date,
    candidate_sid,
    resource_screening_package_num,
    sequence_num,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CAST(xwlk.sk AS INT64) AS candidate_onboarding_event_sid,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
  '2' AS event_type_id,
  req.recruitment_requisition_num_text AS recruitment_requisition_num_text,
  DATETIME(stg.completiondate) AS completed_date,
  can.candidate_sid AS candidate_sid,
  orc.resource_screening_package_num AS resource_screening_package_num,
  CAST(NULL AS INT64) AS sequence_num,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_hcm_resourcetransition_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  CONCAT(stg.jobrequisition, stg.candidate, '2', '-B') = xwlk.sk_source_txt
  AND UPPER(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS req
ON
  req.requisition_num = CAST(stg.jobrequisition AS INT64)
  AND req.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(req.source_system_code) = 'B'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate AS can
ON
  can.candidate_num = CAST(stg.candidate AS INT64)
  AND can.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(can.source_system_code) = 'B'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_resource AS orc
ON
  orc.candidate_sid = can.candidate_sid
  AND orc.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(orc.source_system_code) = 'B'
WHERE
  UPPER(stg.status_state) = 'COMPLETE' QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobrequisition, stg.candidate ORDER BY stg.update_stamp_timestamp DESC, stg.completiondate DESC, orc.resource_screening_package_num DESC) = 1
UNION DISTINCT
SELECT
  CAST(xwlk_0.sk AS INT64) AS candidate_onboarding_event_sid,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
  '1' AS event_type_id,
  req_0.recruitment_requisition_num_text AS recruitment_requisition_num_text,
  DATETIME(stg_0.hcaresourcescreeningvendorstatusdate) AS completed_date,
  orc_0.candidate_sid AS candidate_sid,
  CAST(stg_0.resourcescreeningpackage AS INT64) AS resource_screening_package_num,
  CAST(stg_0.sequencenumber AS INT64) AS sequence_num,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_resourcescreening_stg AS stg_0
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk_0
ON
  UPPER(CONCAT(stg_0.resourcescreeningpackage, stg_0.sequencenumber, '1', '-B')) = UPPER(xwlk_0.sk_source_txt)
  AND UPPER(xwlk_0.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_resource AS orc_0
ON
  orc_0.resource_screening_package_num = CAST(stg_0.resourcescreeningpackage AS INT64)
  AND orc_0.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(orc_0.source_system_code) = 'B'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS req_0
ON
  req_0.recruitment_requisition_sid = orc_0.recruitment_requisition_sid
  AND req_0.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND UPPER(req_0.source_system_code) = 'B'
WHERE
  UPPER(stg_0.hcaresourcescreeningvendorstatus) = 'COMPLETED'
  AND UPPER(stg_0.screening) = 'DRUGSCREEN' QUALIFY ROW_NUMBER() OVER (PARTITION BY resource_screening_package_num, stg_0.sequencenumber ORDER BY stg_0.update_stamp_timestamp DESC) = 1
UNION DISTINCT
SELECT
  CAST(xwlk_1.sk AS INT64) AS candidate_onboarding_event_sid,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
  stg_1.event_type_id AS event_type_id,
  stg_1.recruitment_requisition_num_text AS recruitment_requisition_num_text,
  stg_1.creation_date_time AS completed_date,
  stg_1.candidate_sid AS candidate_sid,
  NULL AS resource_screening_package_num,
  CAST(NULL AS INT64) AS sequence_num,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_stg AS stg_1
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk_1
ON
  CONCAT(stg_1.requisition_num, stg_1.candidate_num, stg_1.event_type_id, '-B') = xwlk_1.sk_source_txt
  AND UPPER(xwlk_1.sk_type) = 'CANDIDATE_ONBOARDING_EVENT' QUALIFY ROW_NUMBER() OVER (PARTITION BY stg_1.requisition_num, stg_1.candidate_num ORDER BY stg_1.creation_date_time DESC) = 1 ; 
  -- Inserting Rejected Records which are filtered above
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg_reject (ticketcode,
    userid,
    archived,
    casetype,
    ticketstatus,
    createddatetime,
    sladate,
    closeddatetime,
    lasteditdatetime,
    resolveddatetime,
    topic,
    category,
    subcategory,
    source_code,
    servicegroup,
    substatus,
    isfirstcallresolution,
    creatoruserid,
    closeuserid,
    lastedituserid,
    owneruserid,
    chatagentuserid,
    regardinguserid,
    creatorfirstname,
    creatorlastname,
    creatorname,
    contactmethod,
    contactname,
    contactrelationshipname,
    aboutee,
    email,
    firstname,
    lastname,
    surveydatetime,
    surveyid,
    surveyanswer1,
    surveyanswer2,
    surveyanswer3,
    surveyanswer4,
    surveyanswer5,
    surveyscore,
    surveyagreementresponse,
    population,
    priority,
    processtime,
    reminderdatetime,
    reminderemail,
    reminderphone,
    secure,
    showtoee,
    customcheckbox1,
    customcheckbox2,
    customcheckbox3,
    customcheckbox4,
    customcheckbox5,
    customcheckbox6,
    customdate1,
    customdate2,
    customdate3,
    customdate4,
    customdate5,
    customdate6,
    customselect1,
    customselect2,
    customselect3,
    customselect4,
    customselect5,
    customselect6,
    customstring1,
    customstring2,
    customstring3,
    customstring4,
    customstring5,
    customstring6,
    surveyfollowup,
    knowledgedomain,
    remindernote,
    surveycommentresponse,
    subject,
    resolution,
    issue,
    dw_last_update_date_time)
SELECT
  DISTINCT iq.ticketcode,
  iq.userid,
  iq.archived,
  iq.casetype,
  iq.ticketstatus,
  iq.createddatetime,
  iq.sladate,
  iq.closeddatetime,
  iq.lasteditdatetime,
  iq.resolveddatetime,
  iq.topic,
  iq.category,
  iq.subcategory,
  iq.source_code,
  iq.servicegroup,
  iq.substatus,
  iq.isfirstcallresolution,
  iq.creatoruserid,
  iq.closeuserid,
  iq.lastedituserid,
  iq.owneruserid,
  iq.chatagentuserid,
  iq.regardinguserid,
  iq.creatorfirstname,
  iq.creatorlastname,
  iq.creatorname,
  iq.contactmethod,
  iq.contactname,
  iq.contactrelationshipname,
  iq.aboutee,
  iq.email,
  iq.firstname,
  iq.lastname,
  iq.surveydatetime,
  iq.surveyid,
  iq.surveyanswer1,
  iq.surveyanswer2,
  iq.surveyanswer3,
  iq.surveyanswer4,
  iq.surveyanswer5,
  iq.surveyscore,
  iq.surveyagreementresponse,
  iq.population,
  iq.priority,
  iq.processtime,
  iq.reminderdatetime,
  iq.reminderemail,
  iq.reminderphone,
  iq.secure,
  iq.showtoee,
  iq.customcheckbox1,
  iq.customcheckbox2,
  iq.customcheckbox3,
  iq.customcheckbox4,
  iq.customcheckbox5,
  iq.customcheckbox6,
  iq.customdate1,
  iq.customdate2,
  iq.customdate3,
  iq.customdate4,
  iq.customdate5,
  iq.customdate6,
  iq.customselect1,
  iq.customselect2,
  iq.customselect3,
  iq.customselect4,
  iq.customselect5,
  iq.customselect6,
  iq.customstring1,
  iq.customstring2,
  iq.customstring3,
  iq.customstring4,
  iq.customstring5,
  iq.customstring6,
  iq.surveyfollowup,
  iq.knowledgedomain,
  iq.remindernote,
  iq.surveycommentresponse,
  iq.subject,
  iq.resolution,
  iq.issue,
  iq.dw_last_update_date_time
FROM (
  SELECT
    DISTINCT TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg.subject), 1, STRPOS(REVERSE(stg.subject), '-') - 1)))) AS requisition_number,
    TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg.subject), STRPOS(REVERSE(stg.subject), '-') + 1, 5)))) AS process_level_code,
    stg.*
  FROM
    {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS stg
  WHERE
    UPPER(stg.topic) = 'Z-AUTO-ONBOARDING CONFIRM'
    AND UPPER(stg.category) = 'Z-AUTO-ONBOARDING CONFIRM'
    AND UPPER(stg.subcategory) = 'Z-AUTO-ONBOARDING CONFIRM'
    AND STRPOS(stg.subject, '-') <> 0
    AND UPPER(stg.subject) LIKE 'ONBOARDING ACTION REQUIRED %'
    AND UPPER(stg.subject) NOT LIKE '%ACQ -%'
    AND UPPER(stg.subject) NOT LIKE '%ACQ-%'
    AND UPPER(stg.subject) NOT LIKE '%ACQ %'
    AND UPPER(stg.subject) NOT LIKE '%ACQUISITION%'
  UNION DISTINCT
  SELECT
    DISTINCT TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg_0.subject), 1, STRPOS(REVERSE(stg_0.subject), '-') - 1)))) AS requisition_number,
    TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg_0.subject), STRPOS(REVERSE(stg_0.subject), '-') + 1, 5)))) AS process_level_code,
    stg_0.*
  FROM
    {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS stg_0
  WHERE
    UPPER(stg_0.topic) = 'HIRE MY EMPLOYEES'
    AND UPPER(stg_0.category) = 'HIRING EMPLOYEES'
    AND UPPER(stg_0.subcategory) = 'DRUG SCREEN RESULTS'
    AND UPPER(stg_0.subject) LIKE 'CONFIRMED DS RESULTS%'
    AND UPPER(stg_0.subject) NOT LIKE '%ACQ -%'
    AND UPPER(stg_0.subject) NOT LIKE '%ACQ-%'
    AND UPPER(stg_0.subject) NOT LIKE '%ACQ %'
    AND UPPER(stg_0.subject) NOT LIKE '%ACQUISITION%'
  UNION DISTINCT
  SELECT
    DISTINCT TRIM(REVERSE(TRIM(SUBSTR(REPLACE(REVERSE(stg_1.subject), ')', ''), 1, STRPOS(REPLACE(REVERSE(stg_1.subject), ')', ''), '-') - 1)))) AS requisition_number,
    TRIM(REVERSE(TRIM(SUBSTR(REVERSE(stg_1.subject), STRPOS(REVERSE(stg_1.subject), '-') + 1, 5)))) AS process_level_code,
    stg_1.*
  FROM
    {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS stg_1
  WHERE
    UPPER(stg_1.topic) = 'Z-AUTO-BG CHECK AUTH'
    AND UPPER(stg_1.category) = 'Z-AUTO-BG CHECK AUTH'
    AND UPPER(stg_1.subcategory) = 'Z-AUTO-BG CHECK AUTH'
    AND UPPER(stg_1.subject) LIKE 'BG AUTH COMPLETE%'
    AND UPPER(stg_1.subject) NOT LIKE '%ACQ -%'
    AND UPPER(stg_1.subject) NOT LIKE '%ACQ-%'
    AND UPPER(stg_1.subject) NOT LIKE '%ACQ %'
    AND UPPER(stg_1.subject) NOT LIKE '%ACQUISITION%'
    AND STRPOS(stg_1.subject, '-') <> 0 ) AS iq
WHERE
  LENGTH(iq.requisition_number) = 0
  OR LENGTH(iq.process_level_code) = 0
  OR LOWER(iq.requisition_number) <> UPPER(iq.requisition_number)
  OR LOWER(iq.process_level_code) <> UPPER(iq.process_level_code)
  OR iq.requisition_number LIKE '%*%'
  OR iq.requisition_number LIKE '%/%'
  OR iq.requisition_number LIKE '%)%'
  OR iq.requisition_number LIKE '%(%'
  OR iq.requisition_number LIKE '%.%'
  OR iq.requisition_number LIKE '%]%'
  OR iq.requisition_number LIKE '%[%'
  OR iq.requisition_number LIKE '% %'
  OR iq.requisition_number LIKE '%:%'
  OR iq.requisition_number LIKE '%\'%' ;