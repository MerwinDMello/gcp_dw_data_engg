  /*  Truncate Worktable Table     */
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk; 
  /*  LOAD WORK TABLE WITH WORKING DATA */
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.file_date,
  ofr.offer_sid,
  stg.udfdefinition_entity AS element_detail_entity_text,
  stg.udfdefinition_id AS element_detail_type_text,
  cast(stg.sequence as int64) AS element_detail_seq_num,
  DATETIME(stg.file_date) AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  cast(stg.udselement_number as int64) AS element_detail_id,
  stg.value AS element_detail_value_text,
  'T' AS source_system_code,
  TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.taleo_offerudf AS stg
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.offer AS ofr
ON
  CASE TRIM(stg.offer_number)
    WHEN '' THEN 0
  ELSE
  CAST(TRIM(stg.offer_number) AS INT64)
END
  = ofr.offer_num
  AND (ofr.valid_to_date) = DATETIME("9999-12-31 23:59:59")
GROUP BY
  1,
  2,
  3,
  4,
  5,
  1,
  7,
  8,
  9,
  10 ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  '25_Time_Accrua' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date,
  NULL AS element_detail_id,
  TRIM(stg.hcajobapplicationtimeaccrual) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND TRIM(stg.hcajobapplicationtimeaccrual) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  '90_Spec_Bn_Ind' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  NULL AS element_detail_id,
  TRIM(stg.hcajobapplicationspecstatusind) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND TRIM(stg.hcajobapplicationspecstatusind) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'BSN_Completion_Period' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  NULL AS element_detail_id,
  stg.hcajobapplicationbsncompletionperiod_state AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND stg.hcajobapplicationbsncompletionperiod_state NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'BSN_Enrollment_Period' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  TRIM(stg.hcajobapplicationbsnenrollmentperiod_state) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND TRIM(stg.hcajobapplicationbsnenrollmentperiod_state) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'Date of initiation of Benefits' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  cast(stg.hcajobapplicationbenefitswaitingperiod as string) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND cast(stg.hcajobapplicationbenefitswaitingperiod as string) NOT IN( '',    '0',    '.00', '0.0' ) 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'FTE_Offer' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  cast(rj.fte_pct as string) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj
ON
  stg.jobrequisition = rj.recruitment_job_num
  AND rj.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  stg.offerstatus <> 0
  AND cast(rj.fte_pct as string) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'GraduateNurseProgramCompletionDate' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  cast(stg.hcajobapplicationrntrainingprogramcompletiondate as string) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND cast(stg.hcajobapplicationrntrainingprogramcompletiondate as string) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'GDS_Exempt_Pct' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  cast(rp.gsd_pct as string) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj
ON
  stg.jobrequisition = rj.recruitment_job_num
  AND rj.valid_to_date = DATETIME("9999-12-31 23:59:59")
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_position AS rp
ON
  rj.recruitment_position_sid = rp.recruitment_position_sid
  AND rp.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  stg.offerstatus <> 0
  AND cast(rp.gsd_pct as string) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'Incentive_Pct_Payout' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  cast(rp.incentive_payout_pct as string) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj
ON
  stg.jobrequisition = rj.recruitment_job_num
  AND rj.valid_to_date = DATETIME("9999-12-31 23:59:59")
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_position AS rp
ON
  rj.recruitment_position_sid = rp.recruitment_position_sid
  AND rp.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  stg.offerstatus <> 0
  AND cast(rp.incentive_payout_pct as string) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'Incentive_Plan_Name' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  TRIM(rp.incentive_plan_name) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj
ON
  stg.jobrequisition = rj.recruitment_job_num
  AND rj.valid_to_date = DATETIME("9999-12-31 23:59:59")
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_position AS rp
ON
  rj.recruitment_position_sid = rp.recruitment_position_sid
  AND rp.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  stg.offerstatus <> 0
  AND TRIM(rp.incentive_plan_name) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'Incentive_Plan_Potential' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  cast(rp.incentive_plan_potential_pct as string) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj
ON
  stg.jobrequisition = rj.recruitment_job_num
  AND rj.valid_to_date = DATETIME("9999-12-31 23:59:59")
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_position AS rp
ON
  rj.recruitment_position_sid = rp.recruitment_position_sid
  AND rp.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  stg.offerstatus <> 0
  AND cast(rp.incentive_plan_potential_pct as string) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'Internal_External_Candidate' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  TRIM(stg.hcajobapplicationintextcandidate_state) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND TRIM(stg.hcajobapplicationintextcandidate_state) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'Job_Experience_Date' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  cast(stg.hcajobapplicationjobexperiencedate as string),
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND cast(stg.hcajobapplicationjobexperiencedate as string) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'New - Graduate Nurse Recruitment Tracking' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  TRIM(stg.hcajobapplicationnewgrad) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND TRIM(stg.hcajobapplicationnewgrad) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'NewProgramSpecialty' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  TRIM(stg.hcajobapplicationrntrainingprogramspecialty) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND TRIM(stg.hcajobapplicationrntrainingprogramspecialty) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'Overtime Status (offer)' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  TRIM(ros.overtime_status_desc) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj
ON
  stg.jobrequisition = rj.recruitment_job_num
  AND rj.valid_to_date = DATETIME("9999-12-31 23:59:59")
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_overtime_status AS ros
ON
  rj.overtime_status_id = ros.overtime_status_id
WHERE
  stg.offerstatus <> 0
  AND UPPER(TRIM(ros.overtime_status_desc)) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'Promotional_Grants' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  cast(stg.hcajobapplicationinternalpromotionalgrant as string) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND cast(stg.hcajobapplicationinternalpromotionalgrant as string) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'Recruiter_Title' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  TRIM(stg1.description) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS stg1
ON
  stg.jobrequisition = stg1.jobrequisition
WHERE
  stg.offerstatus <> 0
  AND TRIM(stg1.description) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'ReloAmount' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
cast(stg.hcajobapplicationrelocationbonus as string) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND cast(stg.hcajobapplicationrelocationbonus as string) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'RN_Acute_Exp_Date' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  cast(stg.hcajobapplicationrnexperiencedate as string) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND cast(stg.hcajobapplicationrnexperiencedate as string) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'Start_Location' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  TRIM(rrl.location_code_text) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj
ON
  stg.jobrequisition = rj.recruitment_job_num
  AND rj.valid_to_date = DATETIME("9999-12-31 23:59:59")
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_location AS rrl
ON
  rj.primary_facility_location_num = rrl.location_num
WHERE
  stg.offerstatus <> 0
  AND TRIM(rrl.location_code_text) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'RN_Training_Program' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  TRIM(stg.hcajobapplicationrntrainingprogram) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND UPPER(TRIM(stg.hcajobapplicationrntrainingprogram)) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk (file_date,
    offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CURRENT_DATE('US/Central') AS file_date,
  cast(xwlk.sk as int64) AS offer_sid,
  'OFFER' AS element_detail_entity_text,
  'RN_License_Type' AS element_detail_type_text,
  1 AS element_detail_seq_num,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) as  valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date ,
  NULL AS element_detail_id,
  TRIM(stg.hcanurselictype) AS element_detail_value_text,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(CONCAT(stg.jobapplication, '-', stg.candidate, '-', stg.jobrequisition, '-ATS')) = TRIM(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'OFFER'
WHERE
  stg.offerstatus <> 0
  AND UPPER(TRIM(stg.hcanurselictype)) NOT IN( '',
    '0',
    '.00', '0.0' ) QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg._asoftimestamp DESC) = element_detail_seq_num ;
	