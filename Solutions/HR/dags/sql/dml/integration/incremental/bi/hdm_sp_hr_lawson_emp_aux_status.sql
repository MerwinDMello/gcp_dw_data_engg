BEGIN

DECLARE
  current_ts datetime;
  set current_ts = current_datetime('US/Central');

TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.aux_status;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.aux_status (employee_sid,
    status_from_date,
    status_to_date,
    aux_status_code,
    aux_status_sid,
    rr1,
    rr2,
    lawson_company_num,
    dw_last_update_date_time)
SELECT
  hreh.employee_sid,
  hreh.hr_employee_element_date AS status_from_date,
  COALESCE(a.hr_employee_element_date - 1, DATE '9999-12-31') AS status_to_date,
  hreh.aux_status_code,
  hreh.aux_status_sid,
  hreh.row_rank AS rr1,
  a.row_rank AS rr2,
  hreh.lawson_company_num,
  datetime_TRUNC(current_ts, SECOND) AS dw_last_update_date_time
FROM (
  SELECT
    hreh_0.employee_sid,
    hreh_0.hr_employee_element_date,
    hr_employee_value_alphanumeric_text AS aux_status_code,
    sta.status_sid AS aux_status_sid,
    hreh_0.lawson_company_num,
    ROW_NUMBER() OVER (PARTITION BY hreh_0.employee_sid ORDER BY hreh_0.hr_employee_element_date, sequence_num) AS row_rank
  FROM
    {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS hreh_0
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.status AS sta
  ON
    hreh_0.hr_employee_value_alphanumeric_text = sta.status_code
    AND hreh_0.lawson_company_num = sta.lawson_company_num
    AND upper(trim((sta.status_type_code))) = 'AUX'
    AND DATE(sta.valid_to_date) = '9999-12-31'
  WHERE
    hreh_0.lawson_element_num = 2088
    AND DATE(hreh_0.valid_to_date) = '9999-12-31'
    AND hreh_0.lawson_company_num <> 300 ) AS hreh
LEFT OUTER JOIN (
  SELECT
    hreh_0.employee_sid,
    hreh_0.hr_employee_element_date,
    hreh_0.hr_employee_value_alphanumeric_text,
    ROW_NUMBER() OVER (PARTITION BY hreh_0.employee_sid ORDER BY hreh_0.hr_employee_element_date, hreh_0.sequence_num) AS row_rank
  FROM
    {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS hreh_0
  WHERE
    hreh_0.lawson_element_num = 2088
    AND DATE(hreh_0.valid_to_date) = '9999-12-31' ) AS a
ON
  hreh.employee_sid = a.employee_sid
  AND hreh.row_rank + 1 = a.row_rank ;

TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.emp_status;


INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.emp_status (employee_sid,
    status_from_date,
    status_to_date,
    emp_status_code,
    emp_status_sid,
    rr1,
    rr2,
    lawson_company_num,
    dw_last_update_date_time)
SELECT
  hreh.employee_sid,
  hreh.hr_employee_element_date AS status_from_date,
  COALESCE(a.hr_employee_element_date - 1, DATE '9999-12-31') AS status_to_date,
  hreh.emp_status_code,
  hreh.emp_status_sid,
  hreh.row_rank AS rr1,
  a.row_rank AS rr2,
  hreh.lawson_company_num,
  datetime_TRUNC(current_ts, SECOND) AS dw_last_update_date_time
FROM (
  SELECT
    hreh_0.employee_sid,
    hreh_0.hr_employee_element_date,
    hreh_0.hr_employee_value_alphanumeric_text AS emp_status_code,
    ste.status_sid AS emp_status_sid,
    hreh_0.lawson_company_num,
    ROW_NUMBER() OVER (PARTITION BY hreh_0.employee_sid ORDER BY hreh_0.hr_employee_element_date, hreh_0.sequence_num) AS row_rank
  FROM
    {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS hreh_0
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.status AS ste
  ON
    hreh_0.hr_employee_value_alphanumeric_text = ste.status_code
    AND hreh_0.lawson_company_num = ste.lawson_company_num
    AND upper(trim(ste.status_type_code)) = 'EMP'
    AND DATE(ste.valid_to_date) = '9999-12-31'
  WHERE
    hreh_0.lawson_element_num = 20
    AND DATE(hreh_0.valid_to_date) = '9999-12-31'
    AND hreh_0.lawson_company_num <> 300 ) AS hreh
LEFT OUTER JOIN (
  SELECT
    hreh_0.employee_sid,
    hreh_0.hr_employee_element_date,
    hreh_0.hr_employee_value_alphanumeric_text,
    ROW_NUMBER() OVER (PARTITION BY hreh_0.employee_sid ORDER BY hreh_0.hr_employee_element_date, hreh_0.sequence_num) AS row_rank
  FROM
    {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS hreh_0
  WHERE
    hreh_0.lawson_element_num = 20
    AND DATE(hreh_0.valid_to_date) = '9999-12-31' ) AS a
ON
  hreh.employee_sid = a.employee_sid
  AND hreh.row_rank + 1 = a.row_rank ;

END;
