BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE
  current_ts datetime;
  set current_ts = CURRENT_datetime('US/Central');
BEGIN TRANSACTION;
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
SELECT
  0 AS employee_sid,
  reqday.requisition_sid,
  reqday.position_sid,
  reqday.date_id,
  dm.analytics_msr_sid,
  reqday.dept_sid,
  jc.job_class_sid,
  jc.job_code_sid,
  reqday.working_location_code AS location_code,
  gldc.coid,
  gldc.company_code,
  sf.functional_dept_num,
  sf.sub_functional_dept_num,
  0 AS auxiliary_status_sid,
  0 AS employee_status_sid,
  COALESCE(rkeyt1.key_talent_id, rkeyt2.key_talent_id, rkeyt3.key_talent_id, rkeyt4.key_talent_id, rkeyt5.key_talent_id, rkeyt6.key_talent_id, rkeyt7.key_talent_id, rkeyt8.key_talent_id) AS key_talent_id,
  COALESCE(mat1.integrated_lob_id, mat4.integrated_lob_id, mat3.integrated_lob_id, mat2.integrated_lob_id) AS integrated_lob_id,
  '0' AS action_code,
  '0' AS action_reason_text,
  reqday.lawson_company_num,
  reqday.process_level_code,
  reqday.work_schedule_code,
  0 AS recruiter_owner_user_sid,
  COALESCE(reqday.end_date, reqday.requisition_open_date) AS requisition_approval_date,
  0 AS employee_num,
  1 AS metric_numerator_qty,
  0 AS metric_denominator_qty,
  'L' AS source_system_code,
  datetime_TRUNC(current_ts, SECOND) AS dw_last_update_date_time
FROM (
  SELECT
    r.requisition_sid,
    rp.position_sid,
    r.process_level_code,
    r.lawson_company_num,
    r.location_code AS working_location_code,
    r.work_schedule_code,
    rd.dept_sid,
    rs.status_sid,
    rs.status_code,
    d.date_id,
    r.requisition_open_date,
    rw.end_date
  FROM
    {{ params.param_hr_base_views_dataset_name }}.requisition AS r
  LEFT OUTER JOIN (
    SELECT
      requisition_workflow.requisition_sid,
      MAX(requisition_workflow.end_date) AS end_date
    FROM
      {{ params.param_hr_base_views_dataset_name }}.requisition_workflow
    WHERE
      DATE(requisition_workflow.valid_to_date) = '9999-12-31'
      AND requisition_workflow.end_date <> '9999-12-31'
    GROUP BY
      1 ) AS rw
  ON
    r.requisition_sid = rw.requisition_sid
  INNER JOIN
    {{ params.param_pub_views_dataset_name }}.lu_date AS d
  ON
    d.date_id >=
    CASE
      WHEN rw.end_date <= r.requisition_closed_date THEN COALESCE(rw.end_date, r.requisition_open_date)
    ELSE
    r.requisition_open_date
  END
    AND d.date_id <= r.requisition_closed_date
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.requisition_status AS rs
  ON
    r.requisition_sid = rs.requisition_sid
    AND DATE(rs.valid_to_date) = '9999-12-31'
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp
  ON
    rp.requisition_sid = r.requisition_sid
    AND DATE(rp.valid_to_date) = '9999-12-31'
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd
  ON
    rd.requisition_sid = r.requisition_sid
    AND DATE(rd.valid_to_date) = '9999-12-31'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr
  ON
    r.requisition_sid = rr.lawson_requisition_sid
    AND DATE(rr.valid_to_date) = '9999-12-31'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.ref_workflow AS rwo
  ON
    rr.workflow_id = rwo.workflow_id
  WHERE
    (r.requisition_closed_date) <> '9999-12-31'
    AND UPPER(trim(rs.status_code)) <> 'WFINPROG'
    AND DATE(r.valid_to_date) = '9999-12-31'
    AND (d.date_id) >= '2016-01-01'
    AND (rwo.workflow_code IS NULL
      OR UPPER(trim(rwo.workflow_code)) NOT LIKE '%ACQ%')
  UNION DISTINCT
  SELECT
    r.requisition_sid,
    rp.position_sid,
    r.process_level_code,
    r.lawson_company_num,
    r.location_code AS working_location_code,
    r.work_schedule_code,
    rd.dept_sid,
    rs.status_sid,
    rs.status_code,
    d.date_id,
    r.requisition_open_date,
    rw.end_date
  FROM
    {{ params.param_hr_base_views_dataset_name }}.requisition AS r
  LEFT OUTER JOIN (
    SELECT
      requisition_workflow.requisition_sid,
      MAX(requisition_workflow.end_date) AS end_date
    FROM
      {{ params.param_hr_base_views_dataset_name }}.requisition_workflow
    WHERE
      DATE(requisition_workflow.valid_to_date) = '9999-12-31'
      AND (requisition_workflow.end_date) <> '9999-12-31'
    GROUP BY
      1 ) AS rw
  ON
    r.requisition_sid = rw.requisition_sid
  INNER JOIN
    {{ params.param_pub_views_dataset_name }}.lu_date AS d
  ON
    d.date_id >=
    CASE
      WHEN rw.end_date <= r.requisition_closed_date THEN COALESCE(rw.end_date, r.requisition_open_date)
    ELSE
    r.requisition_open_date
  END
    AND d.date_id <= current_date('US/Central')
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.requisition_status AS rs
  ON
    r.requisition_sid = rs.requisition_sid
    AND DATE(rs.valid_to_date) = '9999-12-31'
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp
  ON
    rp.requisition_sid = r.requisition_sid
    AND DATE(rp.valid_to_date) = '9999-12-31'
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd
  ON
    rd.requisition_sid = r.requisition_sid
    AND DATE(rd.valid_to_date) = '9999-12-31'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr
  ON
    r.requisition_sid = rr.lawson_requisition_sid
    AND DATE(rr.valid_to_date) = '9999-12-31'
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.ref_workflow AS rwo
  ON
    rr.workflow_id = rwo.workflow_id
  WHERE
    (r.requisition_closed_date) = '9999-12-31'
    AND upper(trim(rs.status_code)) IN( 'WFAPPROVE',
      'REQ-HOLD' )
    AND DATE(r.valid_to_date) = '9999-12-31'
    AND (d.date_id) >= '2016-01-01'
    AND (rwo.workflow_code IS NULL
      OR UPPER(trim(rwo.workflow_code)) NOT LIKE '%ACQ%') ) AS reqday
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.job_position AS jp
ON
  reqday.position_sid = jp.position_sid
  AND reqday.date_id BETWEEN jp.eff_from_date
  AND jp.eff_to_date
  AND DATE(jp.valid_to_date) = '9999-12-31'
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS gldc
ON
  jp.gl_company_num = gldc.gl_company_num
  AND jp.account_unit_num = gldc.account_unit_num
  AND DATE(gldc.valid_to_date) = '9999-12-31'
LEFT OUTER JOIN
  {{ params.param_pub_views_dataset_name }}.fact_facility AS ff
ON
  gldc.coid = ff.coid
  AND gldc.company_code = ff.company_code
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
LEFT OUTER JOIN
  {{ params.param_pub_views_dataset_name }}.functional_department AS df
ON
  sf.functional_dept_num = df.functional_dept_num
INNER JOIN
  {{ params.param_dim_base_views_dataset_name }}.dim_analytics_measure AS dm
ON
  UPPER(trim(dm.analytics_msr_name_child)) = 'HR_LAWSON_VACANCY'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.department AS dept
ON
  reqday.dept_sid = dept.dept_sid
  AND DATE(dept.valid_to_date) = '9999-12-31'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat1
ON
  reqday.process_level_code = mat1.process_level_code
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
  AND upper(trim(sf.sub_functional_dept_desc)) = upper(trim(mat3.sub_functional_dept_desc))
  AND (mat3.match_level_num) = 3
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat4
ON
  reqday.process_level_code = mat4.process_level_code
  AND (mat4.match_level_num) = 4
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt1
ON
  rkeyt1.match_level_num = 1
  AND jc.job_code = rkeyt1.job_code
  AND upper(trim(jc.job_code_desc)) = upper(trim(rkeyt1.job_code_desc))
  AND UPPER(trim(jp.position_code_desc)) LIKE 'ACMO%'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt2
ON
  rkeyt2.match_level_num = 2
  AND jc.job_code = rkeyt2.job_code
  AND ff.lob_code = rkeyt2.lob_code
  AND upper(trim(jc.job_code_desc)) = upper(trim(rkeyt2.job_code_desc))
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt3
ON
  rkeyt3.match_level_num = 3
  AND jc.job_code = rkeyt3.job_code
  AND upper(trim(jc.job_code_desc)) = upper(trim(rkeyt3.job_code_desc))
  AND upper(trim(jp.position_code_desc)) = upper(trim(rkeyt3.job_title_text))
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt4
ON
  rkeyt4.match_level_num = 4
  AND jc.job_code = rkeyt4.job_code
  AND upper(trim(jp.position_code_desc)) = upper(trim(rkeyt4.job_title_text))
  AND reqday.process_level_code = rkeyt4.process_level_code
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt5
ON
  rkeyt5.match_level_num = 5
  AND jc.job_code = rkeyt5.job_code
  AND reqday.process_level_code = rkeyt5.process_level_code
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt6
ON
  rkeyt6.match_level_num = 6
  AND jc.job_code = rkeyt6.job_code
  AND reqday.process_level_code = rkeyt6.process_level_code
  AND UPPER(trim(jp.position_code_desc)) LIKE 'DIR PRGM%'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt7
ON
  rkeyt6.match_level_num = 7
  AND jc.job_code = rkeyt7.job_code
  AND reqday.process_level_code = rkeyt7.process_level_code
  AND dept.dept_code BETWEEN '70000'
  AND '79999'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt8
ON
  rkeyt8.match_level_num = 8
  AND jc.job_code = rkeyt8.job_code
  AND upper(trim(jc.job_code_desc)) = upper(trim(rkeyt8.job_code_desc))
WHERE
  reqday.lawson_company_num <> 300 ;
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
