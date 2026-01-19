BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE
  DUP_COUNT1 INT64;
DECLARE
  current_ts datetime;
SET
  current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.employee_supervisor_wrk;
BEGIN TRANSACTION ;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.employee_supervisor_wrk (employee_sid,
    supervisor_sid,
    valid_from_date,
    valid_to_date,
    employee_num,
    supervisor_code,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time)
SELECT
  ee.employee_sid AS employee_sid,
  COALESCE(es.supervisor_sid, 0) AS supervisor_sid,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  cast(trim(cast(empl.employee as string)) as int64) AS employee_num,
  trim(empl.supervisor) AS supervisor_code,
  cast(trim(cast(empl.company as string)) as int64) AS lawson_company_num,
  trim(empl.process_level) AS process_level_code,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.employee AS empl
INNER JOIN (
  SELECT
    employee.employee_sid,
    employee.employee_num,
    employee.lawson_company_num
  FROM
    {{ params.param_hr_base_views_dataset_name }}.employee
  WHERE
    date(employee.valid_to_date) = ("9999-12-31")
    AND UPPER(employee.source_system_code) = 'L'
  GROUP BY
    1,
    2,
    3 ) AS ee
ON
  upper(trim(cast(empl.employee as string))) = upper(trim(cast(ee.employee_num as string)))
  AND upper(trim(cast(empl.company as string))) = upper(trim(cast(ee.lawson_company_num as string)))
LEFT OUTER JOIN (
  SELECT
    supervisor.supervisor_sid,
    supervisor.supervisor_code,
    supervisor.lawson_company_num
  FROM
    {{ params.param_hr_base_views_dataset_name }}.supervisor
  WHERE
    date(supervisor.valid_to_date) = "9999-12-31"
    AND UPPER(supervisor.source_system_code) = 'L'
  GROUP BY
    1,
    2,
    3 ) AS es
ON
  upper(trim(cast(empl.supervisor as string))) = upper(trim(cast(es.supervisor_code as string)))
  AND upper(trim(cast(empl.company as string))) = upper(trim(cast(es.lawson_company_num as string))) ; 
SET
  DUP_COUNT1 = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      employee_sid,
      supervisor_sid,
      valid_from_date
    FROM
      {{ params.param_hr_stage_dataset_name }}.employee_supervisor_wrk
    GROUP BY
      employee_sid,
      supervisor_sid,
      valid_from_date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT1 <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: employee_supervisor_wrk');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
BEGIN TRANSACTION ;
UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_supervisor AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_supervisor_wrk AS stg
WHERE
  tgt.employee_sid = stg.employee_sid
  AND(
  UPPER(TRIM(CAST(tgt.supervisor_sid AS string))) <> UPPER(TRIM(CAST(stg.supervisor_sid AS string)))
  OR UPPER(TRIM(CAST(tgt.supervisor_code AS string))) <> UPPER(TRIM(CAST(stg.supervisor_code AS string)))
  OR UPPER(TRIM(CAST(tgt.process_level_code AS string))) <> UPPER(TRIM(CAST(stg.process_level_code AS string)))
  )
  AND DATE(tgt.valid_to_date) = "9999-12-31"
  AND tgt.source_system_code = 'L';


INSERT INTO
  {{ params.param_hr_core_dataset_name }}.employee_supervisor (employee_sid,
    supervisor_sid,
    valid_from_date,
    valid_to_date,
    employee_num,
    supervisor_code,
    delete_ind,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time)
SELECT
  employee_supervisor_wrk.employee_sid,
  employee_supervisor_wrk.supervisor_sid,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  employee_supervisor_wrk.employee_num,
  employee_supervisor_wrk.supervisor_code,
  'A' AS delete_ind,
  employee_supervisor_wrk.lawson_company_num,
  employee_supervisor_wrk.process_level_code,
  employee_supervisor_wrk.source_system_code,
  employee_supervisor_wrk.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_supervisor_wrk
WHERE
  (UPPER(TRIM(CAST(employee_supervisor_wrk.employee_sid AS string))),
    UPPER(TRIM(CAST(employee_supervisor_wrk.supervisor_sid AS string))),
    UPPER(TRIM(CAST(employee_supervisor_wrk.supervisor_code AS string)))) NOT IN(
  SELECT
    AS STRUCT UPPER(TRIM(CAST(employee_supervisor.employee_sid AS string))),
    UPPER(TRIM(CAST(employee_supervisor.supervisor_sid AS string))),
    UPPER(TRIM(CAST(employee_supervisor.supervisor_code AS string)))
  FROM
    {{ params.param_hr_base_views_dataset_name }}.employee_supervisor
  WHERE
    date(employee_supervisor.valid_to_date) = "9999-12-31"
    AND employee_supervisor.source_system_code = 'L' ) ;


UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_supervisor AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = current_ts
WHERE
  DATE(tgt.valid_to_date) = "9999-12-31"
  AND UPPER(tgt.source_system_code) = 'L'
  AND UPPER(TRIM(CAST(tgt.employee_sid AS string))) NOT IN(
  SELECT
    UPPER(TRIM(CAST(employee_supervisor_wrk.employee_sid AS string)))
  FROM
    {{ params.param_hr_stage_dataset_name }}.employee_supervisor_wrk
  GROUP BY
    1 ) AND tgt.source_system_code = 'L';


UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_supervisor AS tgt
SET
  delete_ind = emp.delete_ind,
  dw_last_update_date_time = current_ts
FROM (
  SELECT
    UPPER(TRIM(CAST(employee.employee_sid AS string))) as employee_sid,
    UPPER(TRIM(CAST(employee.delete_ind AS string))) as delete_ind
  FROM
    {{ params.param_hr_base_views_dataset_name }}.employee
  WHERE
    UPPER(employee.delete_ind) = 'D'
    AND DATE(employee.valid_to_date) = "9999-12-31" ) AS emp
WHERE
  UPPER(TRIM(CAST(tgt.employee_sid AS string))) = UPPER(TRIM(CAST(emp.employee_sid AS string)))
  AND UPPER(tgt.delete_ind) = 'A'
  AND UPPER(tgt.source_system_code) = 'L'; 


UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_supervisor AS tgt
SET
  delete_ind = emp.delete_ind,
  dw_last_update_date_time = current_ts
FROM (
  SELECT
    UPPER(TRIM(CAST(employee.employee_sid AS string))) as employee_sid,
    UPPER(TRIM(CAST(employee.delete_ind AS string))) as delete_ind
  FROM
    {{ params.param_hr_base_views_dataset_name }}.employee
  WHERE
    UPPER(employee.delete_ind) = 'A'
    AND DATE(employee.valid_to_date) = "9999-12-31" ) AS emp
WHERE
  UPPER(TRIM(CAST(tgt.employee_sid AS string))) = UPPER(TRIM(CAST(emp.employee_sid AS string)))
  AND UPPER(tgt.delete_ind) = 'D'
  AND UPPER(tgt.source_system_code) = 'L'; 


SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      employee_sid,
      valid_from_date
    FROM
      {{ params.param_hr_core_dataset_name }}.employee_supervisor
    GROUP BY
      employee_sid,
      valid_from_date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: employee_supervisor');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;