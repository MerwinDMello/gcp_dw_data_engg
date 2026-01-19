BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE
  current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
  
 
BEGIN TRANSACTION;
DELETE
FROM
  {{ params.param_hr_core_dataset_name }}.bi_employee_detail
WHERE
  1=1;
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.bi_employee_detail (employee_sid,
    employee_num,
    employee_first_name,
    employee_last_name,
    employee_middle_name,
    ethnic_origin_code,
    gender_code,
    adjusted_hire_date,
    birth_date,
    acute_experience_start_date,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time)
SELECT
  e.employee_sid,
  e.employee_num,
  ep.employee_first_name,
  ep.employee_last_name,
  ep.employee_middle_name,
  ep.ethnic_origin_code,
  ep.gender_code,
  e.adjusted_hire_date,
  ep.birth_date,
  emp_det.detail_value_date AS acute_experience_start_date,
  e.lawson_company_num,
  e.process_level_code,
  'L' AS source_system_code,
  datetime_TRUNC(current_ts, SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_base_views_dataset_name }}.employee AS e
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.employee_person AS ep
ON
  e.employee_sid = ep.employee_sid
  AND DATE(ep.valid_to_date) = '9999-12-31'
LEFT OUTER JOIN (
  SELECT
    ed.employee_sid,
    ed.employee_detail_code,
    ed.detail_value_date
  FROM
    {{ params.param_hr_base_views_dataset_name }}.employee_detail AS ed
  WHERE
    upper(trim(ed.employee_detail_code)) = '59'
    AND DATE(ed.valid_to_date) = '9999-12-31' ) AS emp_det
ON
  e.employee_sid = emp_det.employee_sid
WHERE
  DATE(e.valid_to_date) = '9999-12-31' ;
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      employee_sid
    FROM
      {{ params.param_hr_core_dataset_name }}.bi_employee_detail
    GROUP BY
      employee_sid
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.bi_employee_detail');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;
