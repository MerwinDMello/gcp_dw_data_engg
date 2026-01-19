BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
BEGIN TRANSACTION;

DELETE
FROM
  {{ params.param_hr_core_dataset_name }}.employee_position_security_role
WHERE
  1=1;
  
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.employee_position_security_role (employee_sid,
    position_sid,
    eff_from_date,
    span_code,
    access_role_code,
    job_code,
    employee_num,
    coid,
    company_code,
    dept_code,
    position_code,
    employee_34_login_code,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time)
SELECT
  e.employee_sid,
  p.position_sid,
  z.effect_date AS eff_from_date,
  TRIM(z.hca_span_code) AS span_code,
  TRIM(z.a_field) AS access_role_code,
  TRIM(z.job_code),
  z.employee AS employee_num,
  TRIM(g.coid),
  TRIM(g.company_code),
  TRIM(CAST(z.hca_dept AS STRING)) AS dept_code,
  TRIM(z.position_ud) AS position_code,
  TRIM(z.comp_nbr) AS employee_34_login_code,
  z.company AS lawson_company_num,
  TRIM(z.process_level) AS process_level_code,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.zvuserdata AS z
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.employee AS e
ON
  UPPER(TRIM(CAST(z.employee AS STRING))) = UPPER(TRIM(CAST(e.employee_num AS STRING)))
  AND UPPER(TRIM(CAST(z.company AS STRING))) = UPPER(TRIM(CAST(e.lawson_company_num AS STRING)))
  AND date(e.valid_to_date) = "9999-12-31"
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.job_position AS p
ON
  UPPER(TRIM(CAST(z.position_ud AS STRING))) = UPPER(TRIM(CAST(p.position_code AS STRING)))
  AND UPPER(TRIM(CAST(z.company AS STRING))) = UPPER(TRIM(CAST(p.lawson_company_num AS STRING)))
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.gl_lawson_dept_crosswalk AS g
ON
  UPPER(TRIM(CAST(e.gl_company_num AS STRING))) = UPPER(TRIM(CAST(g.gl_company_num AS STRING)))
  AND UPPER(TRIM(CAST(e.account_unit_num AS STRING))) = UPPER(TRIM(CAST(g.account_unit_num AS STRING)))
WHERE
  z.effect_date IS NOT NULL
  AND e.employee_sid IS NOT NULL
  AND p.position_sid IS NOT NULL
  AND z.company IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY p.position_sid, e.employee_sid, effect_date ORDER BY p.position_sid, e.employee_sid, effect_date DESC) = 1 ;
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Employee_SID,
      Position_SID,
      Eff_From_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.employee_position_security_role
    GROUP BY
      Employee_SID,
      Position_SID,
      Eff_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.time_entry');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;