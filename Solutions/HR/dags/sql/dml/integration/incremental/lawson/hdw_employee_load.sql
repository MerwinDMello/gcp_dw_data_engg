BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);


CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}','employee',
"cast(company as string)||'-'||cast(employee as string)", 'Employee');

TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.employee_wrk1;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.employee_wrk1 (lawson_company_num,
    process_level_code,
    dept_code,
    employee_num,
    hire_date,
    new_hire_date,
    adjusted_hire_date,
    pay_grade_code,
    anniversary_date,
    termination_date,
    fte_percent,
    pay_grade_schedule_code,
    pay_step_num,
    pay_rate_amt,
    account_unit_num,
    gl_company_num,
    union_code,
    user_level_code,
    location_code,
    employee_34_login_code,
    eff_from_date,
    eff_to_date,
    active_dw_ind,
    security_key_text,
    remote_sw,
    source_system_code,
    dw_last_update_date_time)
SELECT
  empl.company AS lawson_company_num,
  CASE
    WHEN UPPER(TRIM(COALESCE(empl.process_level, ''))) = '' THEN '00000'
  ELSE
  TRIM(empl.process_level)
END
  AS process_level_code,
  empl.department AS dept_code,
  empl.employee AS employee_num,
  empl.date_hired AS hire_date,
  empl.new_hire_date AS new_hire_date,
  empl.adj_hire_date AS adjusted_hire_date,
  empl.pay_grade AS pay_grade_code,
  empl.annivers_date AS anniversary_date,
  empl.term_date AS termination_date,
  empl.nbr_fte AS fte_percent,
  empl.r_schedule AS pay_grade_schedule_code,
  empl.pay_step AS pay_step_num,
  empl.pay_rate AS pay_rate_amt,
  TRIM(empl.hm_acct_unit) AS account_unit_num,
  empl.hm_dist_co AS gl_company_num,
  TRIM(empl.union_code) AS union_code,
  TRIM(empl.user_level) AS user_level_code,
  paemp.locat_code AS location_code,
  substring(paemp.comp_nbr,1,7) AS employee_34_login_code,
  CURRENT_DATE('US/Central') AS eff_from_date,
  DATE '9999-12-31' AS eff_to_date,
  'Y' AS active_dw_ind,
  CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(CAST(empl.company AS string)))), TRIM(CAST(empl.company AS string)), '-',
    CASE
      WHEN TRIM(empl.process_level) IS NULL OR TRIM(empl.process_level) = '' THEN '00000'
    ELSE
    CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(empl.process_level))), TRIM(empl.process_level))
  END
    , '-',
    CASE
      WHEN TRIM(empl.department) IS NULL OR TRIM(empl.department) = '' THEN '00000'
    ELSE
    CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(empl.department))), TRIM(empl.department))
  END
    ) AS security_key_text,
  remote,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.employee AS empl
LEFT OUTER JOIN
  {{ params.param_hr_stage_dataset_name }}.paemployee AS paemp
ON
  TRIM(CAST(empl.company AS string)) = TRIM(CAST(paemp.company AS string))
  AND TRIM(CAST(empl.employee AS string)) = TRIM(CAST(paemp.employee AS string)) ;
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.employee_wrk2;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.employee_wrk2 (lawson_company_num,
    process_level_code,
    employee_num,
    hire_date,
    primary_facility_ind)
SELECT
  zbdemohist.company AS lawson_company_num,
  TRIM(zbdemohist.process_level) AS process_level_code,
  zbdemohist.employee AS employee_num,
  zbdemohist.hire_date,
  TRIM(zbdemohist.primary_fac) AS primary_facility_ind
FROM
  {{ params.param_hr_stage_dataset_name }}.zbdemohist QUALIFY ROW_NUMBER() OVER (PARTITION BY zbdemohist.company, zbdemohist.process_level, zbdemohist.employee, zbdemohist.hire_date ORDER BY zbdemohist.hca_upd_date DESC, zbdemohist.hca_upd_time DESC, zbdemohist.hca_crt_date DESC) = 1 ;
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.employee_wrk3;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.employee_wrk3 (lawson_company_num,
    process_level_code,
    dept_code,
    employee_num,
    hire_date,
    new_hire_date,
    adjusted_hire_date,
    pay_grade_code,
    anniversary_date,
    termination_date,
    fte_percent,
    pay_grade_schedule_code,
    pay_step_num,
    pay_rate_amt,
    account_unit_num,
    gl_company_num,
    union_code,
    user_level_code,
    location_code,
    employee_34_login_code,
    eff_from_date,
    eff_to_date,
    active_dw_ind,
    security_key_text,
    remote_sw,
    source_system_code,
    dw_last_update_date_time,
    primary_facility_ind,
    employee_sid_lkp_val,
    employee_prsn_sid_lkp_val,
    hr_company_sid_lkp_val,
    process_level_sid_lkp_val,
    department_sid_lkp_val)
SELECT
  wrk1.lawson_company_num,
  wrk1.process_level_code,
  wrk1.dept_code,
  wrk1.employee_num,
  wrk1.hire_date,
  wrk1.new_hire_date,
  wrk1.adjusted_hire_date,
  wrk1.pay_grade_code,
  wrk1.anniversary_date,
  wrk1.termination_date,
  wrk1.fte_percent,
  wrk1.pay_grade_schedule_code,
  wrk1.pay_step_num,
  wrk1.pay_rate_amt,
  wrk1.account_unit_num,
  wrk1.gl_company_num,
  wrk1.union_code,
  wrk1.user_level_code,
  wrk1.location_code,
  wrk1.employee_34_login_code,
  wrk1.eff_from_date,
  wrk1.eff_to_date,
  wrk1.active_dw_ind,
  wrk1.security_key_text,
  wrk1.remote_sw,
  wrk1.source_system_code,
  wrk1.dw_last_update_date_time,
  CASE
    WHEN UPPER(wrk1.source_system_code) = 'A' THEN 'Y'
  ELSE
  COALESCE(wrk2.primary_facility_ind, 'N')
END
  AS primary_facility_ind,
  SUBSTR(CONCAT(TRIM(CAST(wrk1.lawson_company_num AS string)), '-', TRIM(CAST(wrk1.employee_num AS string))), 1, 255) AS employee_sid_lkp_val,
  SUBSTR(TRIM(CAST(wrk1.employee_num AS string)), 1, 255) AS employee_prsn_sid_lkp_val,
  SUBSTR(TRIM(CAST(wrk1.lawson_company_num AS string)), 1, 255) AS hr_company_sid_lkp_val,
  SUBSTR(CONCAT(TRIM(CAST(wrk1.lawson_company_num AS string)), '-', TRIM(wrk1.process_level_code)), 1, 255) AS process_level_sid_lkp_val,
  SUBSTR(CONCAT(TRIM(CAST(wrk1.lawson_company_num AS string)), '-', TRIM(wrk1.process_level_code), '-', TRIM(wrk1.dept_code)), 1, 255) AS department_sid_lkp_val
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_wrk1 AS wrk1
LEFT OUTER JOIN
  {{ params.param_hr_stage_dataset_name }}.employee_wrk2 AS wrk2
ON
  wrk1.lawson_company_num = wrk2.lawson_company_num
  AND TRIM(wrk1.process_level_code) = TRIM(wrk2.process_level_code)
  AND wrk1.employee_num = wrk2.employee_num
  AND wrk1.hire_date = wrk2.hire_date ;
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.employee_wrk4;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.employee_wrk4 (sk,
    sk_source_txt,
    sk_type,
    dw_last_update_date_time)
SELECT
  ref_sk_xwlk.sk,
  ref_sk_xwlk.sk_source_txt,
  ref_sk_xwlk.sk_type,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk
WHERE
  upper(ref_sk_xwlk.sk_type) = 'EMPLOYEE';

TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.employee_wrk5;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.employee_wrk5 (employee_sid,
    eff_from_date,
    hr_company_sid,
    lawson_company_num,
    employee_num,
    location_code,
    process_level_sid,
    process_level_code,
    dept_sid,
    dept_code,
    pay_grade_code,
    active_dw_ind,
    employee_34_login_code,
    adjusted_hire_date,
    anniversary_date,
    termination_date,
    hire_date,
    new_hire_date,
    primary_facility_ind,
    fte_percent,
    pay_grade_schedule_code,
    pay_step_num,
    pay_rate_amt,
    account_unit_num,
    gl_company_num,
    union_code,
    user_level_code,
    eff_to_date,
    security_key_text,
    remote_sw,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CAST(xwlk.sk AS int64) AS employee_sid,
  wrk3.eff_from_date AS eff_from_date,
  COALESCE(hri.hr_company_sid, 0) AS hr_company_sid,
  wrk3.lawson_company_num,
  wrk3.employee_num,
  wrk3.location_code,
  COALESCE(epi.process_level_sid, 0) AS process_level_sid,
  wrk3.process_level_code,
  COALESCE(did.dept_sid, 0) AS dept_sid,
  wrk3.dept_code,
  wrk3.pay_grade_code,
  wrk3.active_dw_ind,
  wrk3.employee_34_login_code,
  wrk3.adjusted_hire_date,
  wrk3.anniversary_date,
  wrk3.termination_date,
  wrk3.hire_date,
  wrk3.new_hire_date,
  wrk3.primary_facility_ind,
  wrk3.fte_percent,
  wrk3.pay_grade_schedule_code,
  wrk3.pay_step_num,
  wrk3.pay_rate_amt,
  wrk3.account_unit_num,
  wrk3.gl_company_num,
  wrk3.union_code,
  wrk3.user_level_code,
  wrk3.eff_to_date,
  wrk3.security_key_text,
  wrk3.remote_sw,
  wrk3.source_system_code,
  wrk3.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_wrk3 AS wrk3
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.employee_wrk4 AS xwlk
ON
  TRIM(wrk3.employee_sid_lkp_val) = xwlk.sk_source_txt
  AND UPPER(xwlk.sk_type) = 'EMPLOYEE'
LEFT OUTER JOIN (
  SELECT
    hr_company.hr_company_sid,
    hr_company.lawson_company_num
  FROM
    {{ params.param_hr_base_views_dataset_name }}.hr_company
  WHERE
    DATE(hr_company.valid_to_date) = '9999-12-31'
  GROUP BY
    1,
    2 ) AS hri
ON
  TRIM(CAST(wrk3.lawson_company_num AS string)) = TRIM(CAST(hri.lawson_company_num AS string))
LEFT OUTER JOIN (
  SELECT
    process_level.process_level_sid,
    process_level.lawson_company_num,
    process_level.process_level_code
  FROM
    {{ params.param_hr_base_views_dataset_name }}.process_level
  WHERE
    DATE(process_level.valid_to_date) = '9999-12-31'
  GROUP BY
    1,
    2,
    3 ) AS epi
ON
  TRIM(CAST(wrk3.lawson_company_num AS string)) = TRIM(CAST(epi.lawson_company_num AS string))
  AND TRIM(wrk3.process_level_code) = TRIM(epi.process_level_code)
LEFT OUTER JOIN (
  SELECT
    department.dept_sid,
    department.lawson_company_num,
    department.process_level_code,
    department.security_key_text
  FROM
    {{ params.param_hr_base_views_dataset_name }}.department
  WHERE
    DATE(department.valid_to_date) = '9999-12-31'
  GROUP BY
    1,
    2,
    3,
    4 ) AS did
ON
  TRIM(CAST(wrk3.lawson_company_num AS string)) = TRIM(CAST(did.lawson_company_num AS string))
  AND TRIM(wrk3.process_level_code) = TRIM(did.process_level_code)
  AND TRIM(wrk3.security_key_text) = TRIM(did.security_key_text) ;

BEGIN TRANSACTION;
UPDATE
  {{ params.param_hr_core_dataset_name }}.employee AS empl
SET
  valid_to_date = (current_ts - INTERVAL 1 second),
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_wrk5 AS stg
WHERE
  empl.employee_sid = stg.employee_sid
  AND (UPPER(TRIM(COALESCE(empl.location_code, ''))) <> UPPER(TRIM(COALESCE(stg.location_code, '')))
    OR TRIM(CAST(empl.process_level_sid AS string)) <> TRIM(COALESCE(CAST(stg.process_level_sid AS string), ''))
    OR TRIM(CAST(empl.dept_sid AS string)) <> TRIM(COALESCE(CAST(stg.dept_sid AS string), ''))
    OR TRIM(empl.pay_grade_code) <> TRIM(COALESCE(stg.pay_grade_code, ''))
    OR UPPER(TRIM(COALESCE(empl.employee_34_login_code, ''))) <> UPPER(TRIM(COALESCE(stg.employee_34_login_code, '')))
    OR DATE(empl.adjusted_hire_date) <> DATE(stg.adjusted_hire_date)
    OR DATE(empl.anniversary_date) <> DATE(stg.anniversary_date)
    OR DATE(empl.termination_date) <> DATE(stg.termination_date)
    OR (empl.hire_date) <> (stg.hire_date)
    OR TRIM(CAST(COALESCE(empl.new_hire_date, DATE '9999-12-31') AS STRING)) <> TRIM(CAST(COALESCE(stg.new_hire_date, DATE '9999-12-31') AS STRING))
    OR UPPER(TRIM(COALESCE(empl.primary_facility_ind, ''))) <> UPPER(TRIM(COALESCE(stg.primary_facility_ind, '')))
    OR UPPER(TRIM(COALESCE(CAST(empl.fte_percent AS string), ''))) <> UPPER(TRIM(COALESCE(CAST(stg.fte_percent AS string), '')))
    OR TRIM(empl.pay_grade_schedule_code) <> TRIM(COALESCE(stg.pay_grade_schedule_code, ''))
    OR TRIM(CAST(empl.pay_step_num AS string)) <> TRIM(COALESCE(CAST(stg.pay_step_num AS string), ''))
    OR empl.pay_rate_amt <> stg.pay_rate_amt
    OR UPPER(TRIM(COALESCE(empl.account_unit_num, ''))) <> UPPER(TRIM(COALESCE(stg.account_unit_num, '')))
    OR UPPER(TRIM(COALESCE(empl.union_code, ''))) <> UPPER(TRIM(COALESCE(stg.union_code, '')))
    OR UPPER(TRIM(COALESCE(empl.user_level_code, ''))) <> UPPER(TRIM(COALESCE(stg.user_level_code, '')))
    OR UPPER(TRIM(COALESCE(CAST(empl.gl_company_num AS string), ''))) <> UPPER(TRIM(COALESCE(CAST(stg.gl_company_num AS string), '')))
    OR UPPER(TRIM(COALESCE(CAST(empl.remote_sw AS string), ''))) <> UPPER(TRIM(COALESCE(CAST(stg.remote_sw AS string), ''))))
  AND UPPER(empl.active_dw_ind) = 'Y'
  AND UPPER(empl.source_system_code) = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.employee (employee_sid,
    valid_from_date,
    location_code,
    lawson_company_num,
    employee_num,
    process_level_sid,
    process_level_code,
    dept_sid,
    dept_code,
    pay_grade_code,
    active_dw_ind,
    employee_34_login_code,
    adjusted_hire_date,
    anniversary_date,
    termination_date,
    hire_date,
    new_hire_date,
    primary_facility_ind,
    fte_percent,
    pay_grade_schedule_code,
    pay_step_num,
    pay_rate_amt,
    account_unit_num,
    gl_company_num,
    union_code,
    user_level_code,
    valid_to_date,
    delete_ind,
    security_key_text,
    remote_sw,
    source_system_code,
    dw_last_update_date_time)
SELECT
  employee_wrk5.employee_sid,
  current_ts,
  trim(employee_wrk5.location_code) as location_code,
  employee_wrk5.lawson_company_num,
  employee_wrk5.employee_num,
  employee_wrk5.process_level_sid,
  trim(employee_wrk5.process_level_code) as process_level_code,
  employee_wrk5.dept_sid,
  trim(employee_wrk5.dept_code) as dept_code,
  trim(employee_wrk5.pay_grade_code) as pay_grade_code,
  employee_wrk5.active_dw_ind,
  trim(employee_wrk5.employee_34_login_code) as employee_34_login_code,
  employee_wrk5.adjusted_hire_date,
  employee_wrk5.anniversary_date,
  employee_wrk5.termination_date,
  employee_wrk5.hire_date,
  employee_wrk5.new_hire_date,
  employee_wrk5.primary_facility_ind,
  employee_wrk5.fte_percent,
  trim(employee_wrk5.pay_grade_schedule_code) as pay_grade_schedule_code,
  employee_wrk5.pay_step_num,
  employee_wrk5.pay_rate_amt,
  trim(employee_wrk5.account_unit_num) as account_unit_num,
  employee_wrk5.gl_company_num,
  trim(employee_wrk5.union_code) as union_code,
  trim(employee_wrk5.user_level_code) as user_level_code,
  datetime("9999-12-31 23:59:59"),
  'A' AS delete_ind,
  trim(employee_wrk5.security_key_text),
  employee_wrk5.remote_sw,
  trim(employee_wrk5.source_system_code),
  employee_wrk5.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_wrk5
WHERE
  (TRIM(CAST(employee_wrk5.employee_sid AS string)),
    UPPER(TRIM(COALESCE(employee_wrk5.location_code, ''))),
    TRIM(COALESCE(CAST(employee_wrk5.process_level_sid AS string), '')),
    TRIM(COALESCE(employee_wrk5.process_level_code, '')),
    TRIM(COALESCE(CAST(employee_wrk5.dept_sid AS string), '')),
    TRIM(COALESCE(employee_wrk5.dept_code, '')),
    TRIM(COALESCE(employee_wrk5.pay_grade_code, '')),
    UPPER(TRIM(COALESCE(employee_wrk5.employee_34_login_code, ''))),
    employee_wrk5.adjusted_hire_date,
    employee_wrk5.anniversary_date,
    employee_wrk5.termination_date,
    employee_wrk5.hire_date,
    COALESCE(employee_wrk5.new_hire_date, DATE '9999-12-31'),
    UPPER(TRIM(COALESCE(employee_wrk5.primary_facility_ind, ''))),
    UPPER(TRIM(COALESCE(CAST(employee_wrk5.fte_percent AS string), ''))),
    TRIM(COALESCE(employee_wrk5.pay_grade_schedule_code, '')),
    TRIM(COALESCE(CAST(employee_wrk5.pay_step_num AS string), '')),
    employee_wrk5.pay_rate_amt,
    UPPER(TRIM(COALESCE(employee_wrk5.account_unit_num, ''))),
    UPPER(TRIM(COALESCE(employee_wrk5.union_code, ''))),
    UPPER(TRIM(COALESCE(employee_wrk5.user_level_code, ''))),
    UPPER(TRIM(COALESCE(CAST(employee_wrk5.gl_company_num AS string), ''))),
    UPPER(TRIM(COALESCE(CAST(employee_wrk5.remote_sw AS string), '')))) NOT IN(
  SELECT
    AS STRUCT TRIM(CAST(employee_sid AS string)),
    UPPER(TRIM(COALESCE(location_code, ''))),
    TRIM(CAST(process_level_sid AS string)),
    TRIM(process_level_code),
    TRIM(CAST(dept_sid AS string)),
    TRIM(dept_code),
    TRIM(pay_grade_code),
    UPPER(TRIM(COALESCE(employee_34_login_code, ''))),
    adjusted_hire_date AS adjusted_hire_date,
    anniversary_date AS anniversary_date,
    termination_date AS termination_date,
    hire_date AS hire_date,
    COALESCE(new_hire_date, DATE '9999-12-31'),
    UPPER(TRIM(COALESCE(primary_facility_ind, ''))),
    UPPER(TRIM(COALESCE(CAST(fte_percent AS string), ''))),
    TRIM(pay_grade_schedule_code),
    TRIM(CAST(pay_step_num AS string)),
    pay_rate_amt,
    UPPER(TRIM(COALESCE(account_unit_num, ''))),
    UPPER(TRIM(COALESCE(union_code, ''))),
    UPPER(TRIM(COALESCE(user_level_code, ''))),
    UPPER(TRIM(COALESCE(CAST(gl_company_num AS string), ''))),
    UPPER(TRIM(COALESCE(CAST(remote_sw AS string), '')))
  FROM
    {{ params.param_hr_base_views_dataset_name }}.employee
  WHERE
    UPPER(active_dw_ind) = 'Y' ) 
    ;
UPDATE
  {{ params.param_hr_core_dataset_name }}.employee AS empl
SET
  delete_ind = 'D',
  valid_to_date = (current_ts - INTERVAL 1 second),
  active_dw_ind = 'N'
WHERE
  UPPER(empl.delete_ind) = 'A'
  and UPPER(empl.source_system_code) = 'L'
  AND (empl.lawson_company_num,
    empl.employee_num) NOT IN(
  SELECT
    DISTINCT AS STRUCT employee.company,
    employee.employee
  FROM
    {{ params.param_hr_stage_dataset_name }}.employee )
    AND UPPER(empl.source_system_code) = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.employee AS empl
SET
  delete_ind = 'A'
WHERE
  UPPER(empl.delete_ind) = 'D'
  and UPPER(empl.source_system_code) = 'L'
  AND (empl.lawson_company_num,
    empl.employee_num) IN(
  SELECT
    DISTINCT AS STRUCT employee.company,
    employee.employee
  FROM
    {{ params.param_hr_stage_dataset_name }}.employee )
  AND UPPER(empl.source_system_code) = 'L';


SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Employee_SID ,Valid_From_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.employee
    GROUP BY
      Employee_SID ,Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.employee');
  ELSE
COMMIT TRANSACTION;
END IF
  ;

END
  ;