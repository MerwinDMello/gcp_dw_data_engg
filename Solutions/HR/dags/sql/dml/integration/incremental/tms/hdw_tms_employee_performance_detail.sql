DECLARE current_dt DATETIME;
SET current_dt = datetime_trunc(current_datetime('US/Central'), SECOND);
/*INSERT BAD CHARACTER RECORDS AND THE RECORDS THAT ARE LEFT OUT AFTER INNER JOIN WITH EDWHR.EMPLOYEE_TALENT_PROFILE INTO REJECT*/
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.performance_ratings_report_reject;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.performance_ratings_report_reject
SELECT
  stg.employee_id,
  stg.rev_period,
  stg.rev_period_start_date,
  stg.rev_period_end_date,
  stg.rev_year,
  stg.plan_name,
  stg.eval_workflow_state,
  stg.manager_record,
  stg.manager_employee_id_record,
  stg.emp_perf_rat_num_val,
  stg.emp_perf_rat_scale_val,
  stg.perf_rat_numeric_val,
  stg.perf_rat_scale_val,
  stg.emp_smry_comp_rat_num_val,
  stg.emp_smry_comp_rat_scale_val,
  stg.sumry_comp_rat_numeric_val,
  stg.sumry_comp_rat_scale_val,
  stg.emp_smry_goal_rat_num_val,
  stg.emp_smry_goal_rat_scale_val,
  stg.smry_goal_rating_num_val,
  stg.smry_goal_rat_scale_val,
  stg.emp_strengths_accomplishments,
  stg.emp_area_improvement,
  stg.employee_additional_comments,
  stg.strengths_accomplishments,
  stg.areas_improvement,
  stg.additional_comments,
  stg.evaluation_record_id,
  datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
  CASE
    WHEN TRIM(stg.employee_id) IS NULL THEN 'EMPLOYEE_ID IS NULL'
    WHEN SUBSTR(TRIM(stg.employee_id), 1, 1) NOT IN( '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9' ) THEN 'EMPLOYEE_ID IS ALPHA_NUMERIC'
  ELSE
  CAST(0 AS STRING)
END
  AS reject_reason,
  'EMPLOYEE_PERFORMANCE_DETAIL' AS reject_stg_tbl_nm
FROM
  {{ params.param_hr_stage_dataset_name }}.performance_ratings_report AS stg
WHERE
  SUBSTR(TRIM(stg.employee_id), 1, 1) NOT IN( '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9' )
  OR LENGTH(TRIM(stg.employee_id)) >= 13
  OR SUBSTR(TRIM(stg.employee_id), 1, 1) IN( '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9' )
  AND
  CASE TRIM(stg.employee_id)
    WHEN '' THEN 0
  ELSE
  CAST(REGEXP_EXTRACT(TRIM(stg.employee_id), r'^[0-9]+') AS INT64)
END
  NOT IN(
  SELECT
    employee_num AS employee_num
  FROM
    {{ params.param_hr_core_dataset_name }}.employee ) ;

TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.employee_performance_detail_wrk;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.employee_performance_detail_wrk (evaluation_record_id,
    employee_num,
    employee_sid,
    valid_from_date,
    employee_talent_profile_sid,
    review_period_id,
    review_year_num,
    review_period_start_date,
    review_period_end_date,
    performance_plan_id,
    evaluation_workflow_status_id,
    manager_full_name,
    manager_employee_num,
    employee_performance_num,
    employee_performance_rating_id,
    performance_rating_num,
    performance_rating_id,
    employee_smry_competency_num,
    employee_smry_competency_rating_id,
    smry_competency_num,
    smry_competency_rating_id,
    employee_smry_goal_num,
    employee_smry_goal_rating_id,
    smry_goal_num,
    smry_goal_rating_id,
    employee_strength_accomplishment_text,
    employee_area_of_improvement_text,
    employee_additional_comment_text,
    strength_accomplishment_text,
    areas_of_improvement_text,
    additional_comment_text,
    lawson_company_num,
    process_level_code,
    source_system_key,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.evaluation_record_id,
  SAFE_CAST(stg.employee_id AS INT64) AS employee_num,
  emp.employee_sid AS employee_sid,
  current_dt AS valid_from_date,
  tal.employee_talent_profile_sid,
  per.review_period_id,
  SAFE_CAST(stg.rev_year AS INT64) AS rev_year,
  stg.rev_period_start_date,
  stg.rev_period_end_date,
  plan.performance_plan_id,
  status.performance_status_id,
  stg.manager_record,
  SAFE_CAST(stg.manager_employee_id_record AS INT64) AS manager_employee_num,
  CAST(REGEXP_EXTRACT(stg.emp_perf_rat_num_val, r'^[0-9]+') AS INT64),
  rat.performance_rating_id,
  CASE
    WHEN stg.perf_rat_numeric_val IN( 'NOT RATED', 'NOT APLICABLE' ) THEN CAST(NULL AS INT64)
  ELSE
    CAST(REGEXP_EXTRACT(stg.perf_rat_numeric_val, r'^[0-9]+') AS INT64)
END
  AS performance_rating_num,
  perf.performance_rating_id,
  CAST(CASE
    WHEN UPPER(stg.emp_smry_comp_rat_num_val) = 'NOT COMPUTED' 
    THEN CAST(NULL AS STRING)
  ELSE
  stg.emp_smry_comp_rat_num_val
END AS NUMERIC)
  AS employee_smry_competency_num,
  comp.performance_rating_id,
  CAST(CASE
    WHEN UPPER(stg.sumry_comp_rat_numeric_val) = 'NOT COMPUTED' THEN CAST(NULL AS STRING)
  ELSE
  stg.sumry_comp_rat_numeric_val
END AS NUMERIC)
  AS employee_smry_goal_num,
  scale.performance_rating_id,
  CAST(CASE
    WHEN UPPER(stg.emp_smry_goal_rat_num_val) = 'NOT COMPUTED' THEN CAST(NULL AS STRING)
  ELSE
  stg.emp_smry_goal_rat_num_val
END AS NUMERIC)
  AS smry_competency_num,
  goal.performance_rating_id,
  CAST(CASE
    WHEN UPPER(stg.smry_goal_rating_num_val) = 'NOT COMPUTED' THEN CAST(NULL AS STRING)
  ELSE
  stg.smry_goal_rating_num_val
END AS NUMERIC)
  AS smry_goal_num,
  smry.performance_rating_id,
  stg.emp_strengths_accomplishments,
  stg.emp_area_improvement,
  stg.employee_additional_comments,
  stg.strengths_accomplishments,
  stg.areas_improvement,
  stg.additional_comments,
  COALESCE(tal.lawson_company_num, 0),
  COALESCE(tal.process_level_code, '00000'),
  stg.evaluation_record_id,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  'M' AS source_system_code,
  datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.performance_ratings_report AS stg
INNER JOIN
  {{ params.param_hr_core_dataset_name }}.employee AS emp
ON
  SAFE_CAST(stg.employee_id AS INT64) = emp.employee_num
  AND (emp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  AND emp.lawson_company_num =
  CASE SUBSTR(stg.job_code, 1, 4)
    WHEN '' THEN 0
  ELSE
  CAST(SUBSTR(stg.job_code, 1, 4) AS INT64)
END
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.employee_talent_profile AS tal
ON
  SAFE_CAST(stg.employee_id AS INT64) = tal.employee_num
  AND (tal.valid_to_date) = DATETIME("9999-12-31 23:59:59")
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.ref_performance_period AS per
ON
   Upper(Trim(stg.rev_period)) =  Upper(Trim(per.review_period_desc))
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.ref_performance_plan AS plan
ON
  Upper(Trim(stg.plan_name)) = Upper(Trim(plan.performance_plan_desc))
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.ref_performance_status AS status
ON
  Upper(Trim(stg.eval_workflow_state)) = Upper(Trim(status.performance_status_desc))
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.ref_performance_rating AS rat
ON
  Upper(Trim(stg.emp_perf_rat_scale_val)) = Upper(Trim(rat.performance_rating_desc))
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.ref_performance_rating AS perf
ON
  Upper(Trim(stg.perf_rat_scale_val)) = Upper(Trim(perf.performance_rating_desc))
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.ref_performance_rating AS comp
ON
  Upper(Trim(stg.emp_smry_comp_rat_scale_val)) = Upper(Trim(comp.performance_rating_desc))
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.ref_performance_rating AS scale
ON
  Upper(Trim(stg.sumry_comp_rat_scale_val)) = Upper(Trim(scale.performance_rating_desc))
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.ref_performance_rating AS goal
ON
  Upper(Trim(stg.emp_smry_goal_rat_scale_val)) = Upper(Trim(goal.performance_rating_desc))
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.ref_performance_rating AS smry
ON
  Upper(Trim(stg.smry_goal_rat_scale_val)) = Upper(Trim(smry.performance_rating_desc))
WHERE
  ---isnumeric(COALESCE(stg.employee_id, 0)) = 0 NEED TO CHECK 
  stg.employee_id IS NOT NULL ;

TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.employee_performance_detail_wrk1;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.employee_performance_detail_wrk1 (employee_performance_sid,
    employee_sid,
    employee_num,
    valid_from_date,
    employee_talent_profile_sid,
    review_period_id,
    review_year_num,
    review_period_start_date,
    review_period_end_date,
    performance_plan_id,
    evaluation_workflow_status_id,
    manager_full_name,
    manager_employee_num,
    employee_performance_num,
    employee_performance_rating_id,
    performance_rating_num,
    performance_rating_id,
    employee_smry_competency_num,
    employee_smry_competency_rating_id,
    smry_competency_num,
    smry_competency_rating_id,
    employee_smry_goal_num,
    employee_smry_goal_rating_id,
    smry_goal_num,
    smry_goal_rating_id,
    employee_strength_accomplishment_text,
    employee_area_of_improvement_text,
    employee_additional_comment_text,
    strength_accomplishment_text,
    areas_of_improvement_text,
    additional_comment_text,
    lawson_company_num,
    process_level_code,
    source_system_key,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  ROW_NUMBER() OVER (ORDER BY employee_performance_detail_wrk.evaluation_record_id) AS employee_performance_sid,
  employee_performance_detail_wrk.employee_sid,
  employee_performance_detail_wrk.employee_num,
  current_dt,
  --employee_performance_detail_wrk.valid_from_date,
  employee_performance_detail_wrk.employee_talent_profile_sid,
  employee_performance_detail_wrk.review_period_id,
  employee_performance_detail_wrk.review_year_num,
  employee_performance_detail_wrk.review_period_start_date,
  employee_performance_detail_wrk.review_period_end_date,
  employee_performance_detail_wrk.performance_plan_id,
  employee_performance_detail_wrk.evaluation_workflow_status_id,
  employee_performance_detail_wrk.manager_full_name,
  employee_performance_detail_wrk.manager_employee_num,
  employee_performance_detail_wrk.employee_performance_num,
  employee_performance_detail_wrk.employee_performance_rating_id,
  employee_performance_detail_wrk.performance_rating_num,
  employee_performance_detail_wrk.performance_rating_id,
  employee_performance_detail_wrk.employee_smry_competency_num,
  employee_performance_detail_wrk.employee_smry_competency_rating_id,
  employee_performance_detail_wrk.smry_competency_num,
  employee_performance_detail_wrk.smry_competency_rating_id,
  employee_performance_detail_wrk.employee_smry_goal_num,
  employee_performance_detail_wrk.employee_smry_goal_rating_id,
  employee_performance_detail_wrk.smry_goal_num,
  employee_performance_detail_wrk.smry_goal_rating_id,
  employee_performance_detail_wrk.employee_strength_accomplishment_text,
  employee_performance_detail_wrk.employee_area_of_improvement_text,
  employee_performance_detail_wrk.employee_additional_comment_text,
  employee_performance_detail_wrk.strength_accomplishment_text,
  employee_performance_detail_wrk.areas_of_improvement_text,
  employee_performance_detail_wrk.additional_comment_text,
  employee_performance_detail_wrk.lawson_company_num,
  employee_performance_detail_wrk.process_level_code,
  employee_performance_detail_wrk.source_system_key,
  DATETIME("9999-12-31 23:59:59"),
  --employee_performance_detail_wrk.valid_to_date,
  employee_performance_detail_wrk.source_system_code,
  employee_performance_detail_wrk.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_performance_detail_wrk
GROUP BY
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
  18,
  19,
  20,
  21,
  22,
  23,
  24,
  25,
  26,
  27,
  28,
  29,
  30,
  31,
  32,
  33,
  34,
  35,
  36,
  37,
  employee_performance_detail_wrk.evaluation_record_id ;

UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_performance_detail AS tgt
SET
  valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND),
  dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_performance_detail_wrk1 AS wrk
WHERE
  tgt.employee_performance_sid = wrk.employee_performance_sid
  AND (COALESCE(tgt.employee_sid, 0) <> COALESCE(wrk.employee_sid, 0)
    OR COALESCE(tgt.employee_num, 0) <> COALESCE(wrk.employee_num, 0)
    OR COALESCE(tgt.employee_talent_profile_sid, 0) <> COALESCE(wrk.employee_talent_profile_sid, 0)
    OR COALESCE(tgt.review_period_id, 0) <> COALESCE(wrk.review_period_id, 0)
    OR COALESCE(tgt.review_year_num, 0) <> COALESCE(wrk.review_year_num, 0)
    OR COALESCE(tgt.review_period_start_date, DATE '1900-01-01') <> COALESCE(wrk.review_period_start_date, DATE '1900-01-01')
    OR COALESCE(tgt.review_period_end_date, DATE '1900-01-01') <> COALESCE(wrk.review_period_end_date, DATE '1900-01-01')
    OR COALESCE(tgt.performance_plan_id, 0) <> COALESCE(wrk.performance_plan_id, 0)
    OR COALESCE(tgt.evaluation_workflow_status_id, 0) <> COALESCE(wrk.evaluation_workflow_status_id, 0)
    OR UPPER(COALESCE(TRIM(tgt.manager_full_name), '')) <> UPPER(COALESCE(TRIM(wrk.manager_full_name), ''))
    OR COALESCE(tgt.manager_employee_num, 0) <> COALESCE(wrk.manager_employee_num, 0)
    OR COALESCE(tgt.employee_performance_num, 0) <> COALESCE(wrk.employee_performance_num, 0)
    OR COALESCE(tgt.employee_performance_rating_id, 0) <> COALESCE(wrk.employee_performance_rating_id, 0)
    OR COALESCE(tgt.performance_rating_num, 0) <> COALESCE(wrk.performance_rating_num, 0)
    OR COALESCE(tgt.performance_rating_id, 0) <> COALESCE(wrk.performance_rating_id, 0)
    OR COALESCE(tgt.employee_smry_competency_num, 0) <> COALESCE(wrk.employee_smry_competency_num, 0)
    OR COALESCE(tgt.employee_smry_competency_rating_id, 0) <> COALESCE(wrk.employee_smry_competency_rating_id, 0)
    OR COALESCE(tgt.smry_competency_num, 0) <> COALESCE(wrk.smry_competency_num, 0)
    OR COALESCE(tgt.smry_competency_rating_id, 0) <> COALESCE(wrk.smry_competency_rating_id, 0)
    OR COALESCE(tgt.employee_smry_goal_num, 0) <> COALESCE(wrk.employee_smry_goal_num, 0)
    OR COALESCE(tgt.employee_smry_goal_rating_id, 0) <> COALESCE(wrk.employee_smry_goal_rating_id, 0)
    OR COALESCE(tgt.smry_goal_num, 0) <> COALESCE(wrk.smry_goal_num, 0)
    OR COALESCE(tgt.smry_goal_rating_id, 0) <> COALESCE(wrk.smry_goal_rating_id, 0)
    OR UPPER(COALESCE(tgt.employee_strength_accomplishment_text, '')) <> UPPER(COALESCE(wrk.employee_strength_accomplishment_text, ''))
    OR UPPER(COALESCE(tgt.employee_area_of_improvement_text, '')) <> UPPER(COALESCE(wrk.employee_area_of_improvement_text, ''))
    OR UPPER(COALESCE(tgt.employee_additional_comment_text, '')) <> UPPER(COALESCE(wrk.employee_additional_comment_text, ''))
    OR UPPER(COALESCE(tgt.strength_accomplishment_text, '')) <> UPPER(COALESCE(wrk.strength_accomplishment_text, ''))
    OR UPPER(COALESCE(tgt.areas_of_improvement_text, '')) <> UPPER(COALESCE(wrk.areas_of_improvement_text, ''))
    OR UPPER(COALESCE(tgt.additional_comment_text, '')) <> UPPER(COALESCE(wrk.additional_comment_text, ''))
    OR COALESCE(tgt.lawson_company_num, 0) <> COALESCE(wrk.lawson_company_num, 0)
    OR UPPER(COALESCE(tgt.process_level_code, '')) <> UPPER(COALESCE(wrk.process_level_code, ''))
    OR UPPER(COALESCE(tgt.source_system_key, '')) <> UPPER(COALESCE(wrk.source_system_key, '')));

BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.employee_performance_detail (employee_performance_sid,
    employee_sid,
    employee_num,
    valid_from_date,
    employee_talent_profile_sid,
    review_period_id,
    review_year_num,
    review_period_start_date,
    review_period_end_date,
    performance_plan_id,
    evaluation_workflow_status_id,
    manager_full_name,
    manager_employee_num,
    employee_performance_num,
    employee_performance_rating_id,
    performance_rating_num,
    performance_rating_id,
    employee_smry_competency_num,
    employee_smry_competency_rating_id,
    smry_competency_num,
    smry_competency_rating_id,
    employee_smry_goal_num,
    employee_smry_goal_rating_id,
    smry_goal_num,
    smry_goal_rating_id,
    employee_strength_accomplishment_text,
    employee_area_of_improvement_text,
    employee_additional_comment_text,
    strength_accomplishment_text,
    areas_of_improvement_text,
    additional_comment_text,
    lawson_company_num,
    process_level_code,
    source_system_key,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  wrk.employee_performance_sid,
  wrk.employee_sid,
  wrk.employee_num,
  wrk.valid_from_date,
  wrk.employee_talent_profile_sid,
  wrk.review_period_id,
  wrk.review_year_num,
  wrk.review_period_start_date,
  wrk.review_period_end_date,
  wrk.performance_plan_id,
  wrk.evaluation_workflow_status_id,
  Trim(wrk.manager_full_name),
  wrk.manager_employee_num,
  wrk.employee_performance_num,
  wrk.employee_performance_rating_id,
  wrk.performance_rating_num,
  wrk.performance_rating_id,
  wrk.employee_smry_competency_num,
  wrk.employee_smry_competency_rating_id,
  wrk.smry_competency_num,
  wrk.smry_competency_rating_id,
  wrk.employee_smry_goal_num,
  wrk.employee_smry_goal_rating_id,
  wrk.smry_goal_num,
  wrk.smry_goal_rating_id,
  COALESCE(TRIM(wrk.employee_strength_accomplishment_text),''),
  COALESCE(TRIM(wrk.employee_area_of_improvement_text),''),
  COALESCE(Trim(wrk.employee_additional_comment_text),''),
  COALESCE(Trim(wrk.strength_accomplishment_text),''),
  COALESCE(Trim(wrk.areas_of_improvement_text),''),
  COALESCE(Trim(wrk.additional_comment_text),''),
  wrk.lawson_company_num,
  wrk.process_level_code,
  wrk.source_system_key,
  wrk.valid_to_date,
  wrk.source_system_code,
  wrk.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_performance_detail_wrk1 AS wrk
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.employee_performance_detail AS tgt
ON
  wrk.employee_performance_sid = tgt.employee_performance_sid
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")

WHERE
(COALESCE(tgt.employee_performance_sid, 0)) <> (COALESCE(wrk.employee_performance_sid, 0))
  OR (COALESCE(tgt.employee_talent_profile_sid, 0)) <> (COALESCE(wrk.employee_talent_profile_sid, 0))
  OR (COALESCE(tgt.employee_sid, 0)) <> (COALESCE(wrk.employee_sid, 0))
  OR (COALESCE(tgt.employee_num, 0)) <> (COALESCE(wrk.employee_num, 0))
  OR (COALESCE(tgt.review_period_id, 0)) <> (COALESCE(wrk.review_period_id, 0))
  OR (COALESCE(cast(tgt.review_year_num as string), '1900')) <> (COALESCE(cast(wrk.review_year_num as string), '1900'))
  OR COALESCE(tgt.review_period_start_date, DATE '9999-01-01') <> COALESCE(wrk.review_period_start_date, DATE '9999-01-01')
  OR COALESCE(tgt.review_period_end_date, DATE '9999-01-01') <> COALESCE(wrk.review_period_end_date, DATE '9999-01-01')
  OR (COALESCE(tgt.performance_plan_id, 0)) <> (COALESCE(wrk.performance_plan_id, 0))
  OR (COALESCE(tgt.evaluation_workflow_status_id, 0)) <> (COALESCE(wrk.evaluation_workflow_status_id, 0))
  OR UPPER(COALESCE(Trim(tgt.manager_full_name), '')) <> UPPER(COALESCE(Trim(wrk.manager_full_name), ''))
  OR (COALESCE(tgt.manager_employee_num, 0)) <> (COALESCE(wrk.manager_employee_num, 0))
  OR (COALESCE(tgt.employee_performance_num, 0)) <> (COALESCE(wrk.employee_performance_num, 0))
  OR (COALESCE(tgt.employee_performance_rating_id, 0)) <> (COALESCE(wrk.employee_performance_rating_id, 0))
  OR (COALESCE(tgt.performance_rating_num, 0)) <> (COALESCE(wrk.performance_rating_num, 0))
  OR (COALESCE(tgt.performance_rating_id, 0)) <> (COALESCE(wrk.performance_rating_id, 0))
  OR (COALESCE(tgt.employee_smry_competency_num, 0)) <> (COALESCE(wrk.employee_smry_competency_num, 0))
  OR (COALESCE(tgt.employee_smry_competency_rating_id, 0)) <> (COALESCE(wrk.employee_smry_competency_rating_id, 0))
  OR (COALESCE(tgt.smry_competency_num, 0)) <> (COALESCE(wrk.smry_competency_num, 0))
  OR (COALESCE(tgt.smry_competency_rating_id, 0)) <> (COALESCE(wrk.smry_competency_rating_id, 0))
  OR (COALESCE(tgt.employee_smry_goal_num, 0)) <> (COALESCE(wrk.employee_smry_goal_num, 0))
  OR (COALESCE(tgt.employee_smry_goal_rating_id, 0)) <> (COALESCE(wrk.employee_smry_goal_rating_id, 0))
  OR (COALESCE(tgt.smry_goal_num, 0)) <> (COALESCE(wrk.smry_goal_num, 0))
  OR (COALESCE(tgt.smry_goal_rating_id, 0)) <> (COALESCE(wrk.smry_goal_rating_id, 0))
  OR UPPER(COALESCE(Trim(tgt.employee_strength_accomplishment_text), '')) <> UPPER(COALESCE(Trim(wrk.employee_strength_accomplishment_text), ''))
  OR UPPER(COALESCE(Trim(tgt.employee_area_of_improvement_text), '')) <> UPPER(COALESCE(Trim(wrk.employee_area_of_improvement_text), ''))
  OR UPPER(COALESCE(Trim(tgt.employee_additional_comment_text), '')) <> UPPER(COALESCE(Trim(wrk.employee_additional_comment_text), ''))
  OR UPPER(COALESCE(Trim(tgt.strength_accomplishment_text), '')) <> UPPER(COALESCE(Trim(wrk.strength_accomplishment_text), ''))
  OR UPPER(COALESCE(Trim(tgt.areas_of_improvement_text), '')) <> UPPER(COALESCE(Trim(wrk.areas_of_improvement_text), ''))
  OR UPPER(COALESCE(Trim(tgt.additional_comment_text), '')) <> UPPER(COALESCE(Trim(wrk.additional_comment_text), ''))
  OR (COALESCE(tgt.lawson_company_num, 0)) <> (COALESCE(wrk.lawson_company_num, 0))
  OR UPPER(COALESCE(tgt.process_level_code, '')) <> UPPER(COALESCE(wrk.process_level_code, ''))
  OR UPPER(COALESCE(tgt.source_system_key, '')) <> UPPER(COALESCE(wrk.source_system_key, '')) ;
  
  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT employee_performance_sid ,valid_from_date
      FROM {{ params.param_hr_core_dataset_name }}.employee_performance_detail
      GROUP BY employee_performance_sid ,valid_from_date
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.employee_performance_detail');
  END IF;

UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_performance_detail AS tgt
SET
  valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND),
  dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND)
WHERE
  tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND tgt.employee_performance_sid NOT IN(
  SELECT
    employee_performance_detail_wrk1.employee_performance_sid
  FROM
    {{ params.param_hr_stage_dataset_name }}.employee_performance_detail_wrk1 );

COMMIT TRANSACTION;
END;
