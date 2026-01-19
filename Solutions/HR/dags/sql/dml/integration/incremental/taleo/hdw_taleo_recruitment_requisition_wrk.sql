  /*  GENERATE THE SURROGATE KEYS FOR RECRUITMENT_REQUISITION */
CALL
  {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'taleo_requisition',
    'TRIM(NUMBER)',
    'RECRUITMENT_REQUISITION');
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_wrk; /*  Load Work Table */
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_wrk (file_date,
    recruitment_requisition_sid,
    valid_from_date,
    valid_to_date,
    requisition_num,
    lawson_requisition_sid,
    lawson_requisition_num,
    hiring_manager_user_sid,
    recruitment_requisition_num_text,
    process_level_code,
    approved_sw,
    target_start_date,
    required_asset_num,
    required_asset_sw,
    workflow_id,
    recruitment_job_sid,
    job_template_sid,
    requisition_new_graduate_flag,
    lawson_company_num,
    source_system_code,
    dw_last_update_date_time)
SELECT
  file_date,
  CAST(xwlk.sk as INT64) AS recruitment_requisition_sid,
  file_date AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  CASE TRIM(stg.number)
    WHEN '' THEN 0
  ELSE
  CAST(TRIM(stg.number) AS INT64)
END
  AS requisition_num,
  COALESCE(l_req.requisition_sid, l_req2.requisition_sid) AS lawson_requisition_sid,
  COALESCE(l_req.requisition_num, l_req2.requisition_num) AS lawson_requisition_num,
  hm.recruitment_user_sid AS hiring_manager_user_sid,
  TRIM(stg.contestnumber) AS recruitment_requisition_num_text,
  CASE
    WHEN STRPOS(TRIM(stg.contestnumber), '-') = 4 AND LENGTH(SPLIT(stg.contestnumber, '-')[SAFE_ORDINAL(2)]) = 5 THEN SPLIT(stg.contestnumber, '-')[SAFE_ORDINAL(2)]
    WHEN SUBSTR(stg.contestnumber, 1, 1) NOT IN( '0',
    '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9' ) THEN '00000'
    WHEN STRPOS(stg.contestnumber, '-') = 0 THEN '00000'
  ELSE
  SUBSTR(TRIM(stg.contestnumber), 1, 5)
END
  AS process_level_code,
  CAST(stg.hasbeenapproved as INT64) AS approved_sw,
  CASE
    WHEN TRIM(stg.targetstartdate) = '' THEN CAST(NULL AS DATE)
  ELSE
  PARSE_DATE('%Y-%m-%d', SUBSTR(TRIM(stg.targetstartdate), 1, 10))
END
  AS target_start_date,
  CAST(stg.requestmoreinfoassetvalue as INT64) AS required_asset_num,
  CAST(stg.requestmoreinfobyassetenabled as INT64) AS required_asset_sw,
  NULL AS workflow_id,
  rj.recruitment_job_sid,
  jt.job_template_sid,
  NULL AS requisition_new_graduate_flag,
  ---COALESCE(red.element_code, l_req2.lawson_company_num) AS lawson_company_num,
  COALESCE(CAST(red.element_code AS int64), CAST(l_req2.lawson_company_num AS int64)) AS lawson_company_num,
  'T' AS source_system_code,
  TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.taleo_requisition AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  UPPER(SUBSTR(TRIM(stg.number), 1, 255)) = UPPER(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'RECRUITMENT_REQUISITION'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.job_template AS jt
ON
  (SUBSTR(TRIM(stg.basejobtemplate_number), 1, 255)) = cast(jt.job_template_num as string)
  AND (jt.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(jt.source_system_code) = 'T'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj
ON
  UPPER(SUBSTR(TRIM(stg.jobinformation_number), 1, 255)) = cast(rj.recruitment_job_num as string)
  AND (rj.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(rj.source_system_code) = 'T'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_job_detail AS rjd
ON
  rj.recruitment_job_sid = rjd.recruitment_job_sid
  AND (rjd.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(rjd.element_detail_type_text) = 'HR COMPANY'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_element_detail AS red
ON
  red.element_detail_id = rjd.element_detail_id
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS hm
ON
  UPPER(SUBSTR(TRIM(stg.hiringmanager_userno), 1, 255)) = cast(hm.recruitment_user_num as string)
  AND (hm.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(hm.source_system_code) = 'T'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.requisition AS l_req
ON
  UPPER(SUBSTR(SPLIT(stg.contestnumber, '-')[SAFE_ORDINAL(2)], 1, 20)) = cast(l_req.requisition_num as string)
  AND red.element_code = cast(l_req.lawson_company_num as string)
  AND (l_req.valid_to_date) = DATETIME("9999-12-31 23:59:59")
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.requisition AS l_req2
ON
  TRIM(REPLACE(stg.contestnumber, 'INT-', '')) = CONCAT(TRIM(l_req2.process_level_code), '-', (l_req2.requisition_num))
  AND (l_req2.valid_to_date) = DATETIME("9999-12-31 23:59:59")
GROUP BY
  1,
  2,
  1,
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
  17,
  15,
  19,
  20 ;

CALL  {{ params.param_hr_core_dataset_name }}.sk_gen('{{ params.param_hr_stage_dataset_name }}','ats_cust_jobrequisition_stg',        'jobrequisition ||\'-ATS\'','RECRUITMENT_REQUISITION');
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_wrk (file_date,
    recruitment_requisition_sid,
    valid_from_date,
    valid_to_date,
    requisition_num,
    lawson_requisition_sid,
    lawson_requisition_num,
    hiring_manager_user_sid,
    recruitment_requisition_num_text,
    process_level_code,
    approved_sw,
    target_start_date,
    required_asset_num,
    required_asset_sw,
    workflow_id,
    recruitment_job_sid,
    job_template_sid,
    requisition_new_graduate_flag,
    lawson_company_num,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  SAFE_CAST(xwlk.sk as INT64) AS recruitment_requisition_sid,
  CURRENT_DATE('US/Central') AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  SAFE_CAST(stg.jobrequisition as INT64) AS requisition_num,
  rq.requisition_sid AS lawson_requisition_sid,
  COALESCE(SAFE_CAST(stg.hcajobrequisitionlawsonreqnumber AS int64),0) AS lawson_requisition_num,
  usr.recruitment_user_sid AS hiring_manager_user_sid,
  CONCAT(CASE
      WHEN TRIM(hbstg.hcaorgunitprocesslevel) = '' OR TRIM(hbstg.hcaorgunitprocesslevel) IS NULL THEN '00000'
    ELSE
    TRIM(hbstg.hcaorgunitprocesslevel)
  END
    , '-', stg.jobrequisition) AS recruitment_requisition_num_text,
  CASE
    WHEN TRIM(hbstg.hcaorgunitprocesslevel) = '' OR TRIM(hbstg.hcaorgunitprocesslevel) IS NULL THEN '00000'
  ELSE
  TRIM(hbstg.hcaorgunitprocesslevel)
END
  AS process_level_code,
  CASE
    WHEN SAFE_CAST(stg.status as INT64) = 2 THEN 1
  ELSE
  0
END
  AS approved_sw,
  CAST(stg.dateneeded as DATE) AS target_start_date,
  NULL AS required_asset_num,
  NULL AS required_asset_sw,
  rw.workflow_id,
  rj.recruitment_job_sid,
  NULL AS job_template_sid,
  MAX(CASE
      WHEN UPPER(stg.hcajobrequisitionnewgraduate) = 'YES' THEN 'Y'
      WHEN UPPER(stg.hcajobrequisitionnewgraduate) = 'NO' THEN 'N'
      WHEN UPPER(stg.hcajobrequisitionnewgraduate) = 'UNKNOWN' THEN 'U'
    ELSE
    CAST(NULL AS STRING)
  END
    ) AS requisition_new_graduate_flag,
  hbstg.hcaorgunitcompany AS lawson_company_num,
  'B' AS source_system_code,
  TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  UPPER(TRIM(CONCAT(stg.jobrequisition, '-ATS'))) = UPPER(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'RECRUITMENT_REQUISITION'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj
ON
  rj.recruitment_job_num = SAFE_CAST(stg.jobrequisition as INT64)
  AND (rj.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(rj.source_system_code) = 'B'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS usr
ON
  SAFE_CAST(stg.hiringmanager as INT64) = usr.recruitment_user_num
  AND (usr.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND UPPER(usr.source_system_code) = 'B'
LEFT OUTER JOIN
  {{ params.param_hr_stage_dataset_name }}.ats_cust_hrorganizationunit_stg AS hbstg
ON
  hbstg.hrorganizationunit = SAFE_CAST(stg.hrorganizationunit as INT64)
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.requisition AS rq
ON
  SAFE_CAST(stg.hcajobrequisitionlawsonreqnumber AS INT64) = rq.requisition_num
  AND hbstg.hcaorgunitcompany = rq.lawson_company_num
  AND (rq.valid_to_date) = DATETIME("9999-12-31 23:59:59")
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_workflow AS rw
ON
  TRIM(stg.atsworkflow) = TRIM(rw.workflow_name)
  AND UPPER(rw.source_system_code) = 'B'
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
  17,
  UPPER(CASE
      WHEN UPPER(stg.hcajobrequisitionnewgraduate) = 'YES' THEN 'Y'
      WHEN UPPER(stg.hcajobrequisitionnewgraduate) = 'NO' THEN 'N'
      WHEN UPPER(stg.hcajobrequisitionnewgraduate) = 'UNKNOWN' THEN 'U'
    ELSE
    CAST(NULL AS STRING)
  END
    ),
  19,
  20 ;
