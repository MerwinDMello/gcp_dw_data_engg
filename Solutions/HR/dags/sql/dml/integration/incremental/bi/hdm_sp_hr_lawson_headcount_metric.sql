BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE
  current_ts datetime;
  set current_ts = CURRENT_DATETIME('US/Central');
BEGIN TRANSACTION;
DELETE
FROM
  {{ params.param_hr_core_dataset_name }}.fact_hr_metric_mv
WHERE
  fact_hr_metric_mv.lawson_company_num NOT IN( 300 );
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.fact_hr_metric_mv (employee_sid,
    requisition_sid,
    position_sid,
    date_id,
    analytics_msr_sid,
    dept_sid,
    job_class_sid,
    job_code_sid,
    location_code,
    coid,
    company_code,
    functional_dept_num,
    sub_functional_dept_num,
    auxiliary_status_sid,
    employee_status_sid,
    key_talent_id,
    integrated_lob_id,
    action_code,
    action_reason_text,
    lawson_company_num,
    process_level_code,
    work_schedule_code,
    recruiter_owner_user_sid,
    requisition_approval_date,
    employee_num,
    metric_numerator_qty,
    metric_denominator_qty,
    source_system_code,
    dw_last_update_date_time)
WITH
  pre_emp_day AS(
  SELECT
    ep.eff_to_date,
    ep.eff_from_date,
    ep.employee_sid,
    ep.position_sid,
    ep.employee_num,
    ep.lawson_company_num,
    ep.process_level_code,
    ep.working_location_code,
    ep.dept_sid,
    gldc.coid,
    gldc.company_code,
    jc.job_class_sid,
    jc.job_code,
    jc.job_code_desc,
    jc.job_code_sid,
    jp.position_code_desc,
    sf.functional_dept_num,
    sf.sub_functional_dept_num,
    sf.sub_functional_dept_desc,
    jes.status_sid
  FROM
    {{ params.param_hr_base_views_dataset_name }}.employee AS e
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.employee_position AS ep
  ON
    e.employee_sid = ep.employee_sid
    AND DATE(ep.valid_to_date) = '9999-12-31'
    AND ep.lawson_company_num <> 300
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.junc_employee_status AS jes
  ON
    e.employee_sid = jes.employee_sid
    AND upper(trim(jes.status_type_code)) = 'AUX'
    AND DATE(jes.valid_to_date) = '9999-12-31'
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS gldc
  ON
    ep.gl_company_num = gldc.gl_company_num
    AND ep.account_unit_num = gldc.account_unit_num
    AND DATE(gldc.valid_to_date) = '9999-12-31'
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.job_position AS jp
  ON
    ep.position_sid = jp.position_sid
    AND DATE(jp.valid_to_date) = '9999-12-31'
    AND ep.eff_from_date BETWEEN jp.eff_from_date
    AND jp.eff_to_date
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.job_code AS jc
  ON
    jp.job_code_sid = jc.job_code_sid
    AND DATE(jc.valid_to_date) = '9999-12-31'
  LEFT OUTER JOIN
    {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS sf
  ON
    sf.dept_num = gldc.dept_num
    AND sf.coid = gldc.coid
    AND sf.company_code = gldc.company_code
  WHERE
    DATE(e.valid_to_date) = '9999-12-31'),
  emp_day AS(
  SELECT
    *
  FROM
    pre_emp_day AS ped
  INNER JOIN
    {{ params.param_pub_views_dataset_name }}.lu_date AS d
  ON
    (d.date_id) >= '2016-01-01'
    AND d.date_id BETWEEN ped.eff_from_date
    AND
    CASE
      WHEN (ped.eff_to_date) = '9999-12-31' THEN current_date('US/Central')
    ELSE
    ped.eff_to_date
  END
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ped.employee_sid, ped.position_sid, d.date_id ORDER BY ped.eff_from_date) = 1 )
SELECT
  emp_day.employee_sid,
  0 AS requisition_sid,
  emp_day.position_sid,
  emp_day.date_id,
  dm.analytics_msr_sid,
  emp_day.dept_sid,
  emp_day.job_class_sid,
  emp_day.job_code_sid,
  emp_day.working_location_code AS location_code,
  emp_day.coid,
  emp_day.company_code,
  emp_day.functional_dept_num,
  emp_day.sub_functional_dept_num,
  COALESCE(s.aux_status_sid, emp_day.status_sid) AS auxiliary_status_sid,
  es.emp_status_sid AS employee_status_sid,
  COALESCE(rkeyt1.key_talent_id, rkeyt2.key_talent_id, rkeyt3.key_talent_id, rkeyt4.key_talent_id, rkeyt5.key_talent_id, rkeyt6.key_talent_id, rkeyt7.key_talent_id, rkeyt8.key_talent_id) AS key_talent_id,
  COALESCE(mat1.integrated_lob_id, mat4.integrated_lob_id, mat3.integrated_lob_id, mat2.integrated_lob_id) AS integrated_lob_id,
  '0' AS action_code,
  '0' AS action_reason_text,
  emp_day.lawson_company_num,
  emp_day.process_level_code,
  '0' AS work_schedule_code,
  0 AS recruiter_owner_user_sid,
  CAST(NULL AS date) AS requisition_approval_date,
  emp_day.employee_num,
  1 AS metric_numerator_qty,
  0 AS metric_denominator_qty,
  'L' AS source_system_code,
  DATETIME_TRUNC(current_ts, SECOND) AS dw_last_update_date_time
FROM
  emp_day
LEFT OUTER JOIN
  {{ params.param_hr_stage_dataset_name }}.aux_status AS s
ON
  emp_day.employee_sid = s.employee_sid
  AND emp_day.date_id BETWEEN s.status_from_date
  AND s.status_to_date
  AND upper(trim(s.aux_status_code)) IN( 'PRN',
    'FT',
    'PT',
    'TEMP' )
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.emp_status AS es
ON
  emp_day.employee_sid = es.employee_sid
  AND emp_day.date_id BETWEEN es.status_from_date
  AND es.status_to_date
  AND upper(trim(es.emp_status_code)) IN( '01',
    '02',
    '03',
    '04',
    '05' )
INNER JOIN
  {{ params.param_dim_base_views_dataset_name }}.dim_analytics_measure AS dm
ON
  upper(trim(dm.analytics_msr_name_child)) = 'HR_LAWSON_HEADCOUNT'
LEFT OUTER JOIN
  {{ params.param_pub_views_dataset_name }}.fact_facility AS ff
ON
  emp_day.coid = ff.coid
  AND emp_day.company_code = ff.company_code
LEFT OUTER JOIN
  {{ params.param_pub_views_dataset_name }}.functional_department AS df
ON
  emp_day.functional_dept_num = df.functional_dept_num
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.department AS dept
ON
  emp_day.dept_sid = dept.dept_sid
  AND DATE(dept.valid_to_date) = '9999-12-31'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat1
ON
  emp_day.process_level_code = mat1.process_level_code
  AND dept.dept_code = mat1.dept_code
  AND (mat1.match_level_num) = 1
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat2
ON
  ff.lob_code = mat2.lob_code
  AND ff.sub_lob_code = mat2.sub_lob_code
  AND (mat2.match_level_num) = 2
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat3
ON
  upper(trim(df.functional_dept_desc)) = upper(trim(mat3.functional_dept_desc))
  AND upper(trim(emp_day.sub_functional_dept_desc)) = upper(trim(mat3.sub_functional_dept_desc))
  AND (mat3.match_level_num) = 3
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat4
ON
  emp_day.process_level_code = mat4.process_level_code
  AND (mat4.match_level_num) = 4
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt1
ON
  rkeyt1.match_level_num = 1
  AND emp_day.job_code = rkeyt1.job_code
  AND upper(trim(emp_day.job_code_desc)) = upper(trim(rkeyt1.job_code_desc))
  AND upper(trim(emp_day.position_code_desc)) LIKE 'ACMO%'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt2
ON
  rkeyt2.match_level_num = 2
  AND emp_day.job_code = rkeyt2.job_code
  AND ff.lob_code = rkeyt2.lob_code
  AND upper(trim(emp_day.job_code_desc)) = upper(trim(rkeyt2.job_code_desc))
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt3
ON
  rkeyt3.match_level_num = 3
  AND emp_day.job_code = rkeyt3.job_code
  AND upper(trim(emp_day.job_code_desc)) = upper(trim(rkeyt3.job_code_desc))
  AND upper(trim(emp_day.position_code_desc)) = upper(trim(rkeyt3.job_title_text))
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt4
ON
  rkeyt4.match_level_num = 4
  AND emp_day.job_code = rkeyt4.job_code
  AND upper(trim(emp_day.position_code_desc)) = upper(trim(rkeyt4.job_title_text))
  AND emp_day.process_level_code = rkeyt4.process_level_code
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt5
ON
  rkeyt5.match_level_num = 5
  AND emp_day.job_code = rkeyt5.job_code
  AND emp_day.process_level_code = rkeyt5.process_level_code
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt6
ON
  rkeyt6.match_level_num = 6
  AND emp_day.job_code = rkeyt6.job_code
  AND emp_day.process_level_code = rkeyt6.process_level_code
  AND upper(trim(emp_day.position_code_desc)) LIKE 'DIR PRGM%'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt7
ON
  rkeyt6.match_level_num = 7
  AND emp_day.job_code = rkeyt7.job_code
  AND emp_day.process_level_code = rkeyt7.process_level_code
  AND upper(trim(dept.dept_code)) BETWEEN '70000'
  AND '79999'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt8
ON
  rkeyt8.match_level_num = 8
  AND emp_day.job_code = rkeyt8.job_code
  AND upper(trim(emp_day.job_code_desc)) = upper(trim(rkeyt8.job_code_desc))
WHERE
  emp_day.lawson_company_num <> 300 ;
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Employee_SID,
      Requisition_SID,
      Position_SID,
      Date_Id,
      Analytics_Msr_Sid
    FROM
      {{ params.param_hr_core_dataset_name }}.fact_hr_metric_mv
    GROUP BY
      Employee_SID,
      Requisition_SID,
      Position_SID,
      Date_Id,
      Analytics_Msr_Sid
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.fact_hr_metric_mv');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;
