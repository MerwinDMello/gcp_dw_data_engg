BEGIN
DECLARE
  dup_count int64;
DECLARE
  current_ts datetime;
DECLARE
  lv_par string;
SET
  lv_par = "trim(coalesce(cast(Company as string),'')) ||\'-\'|| trim(coalesce(Code,''))";
SET
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
CALL
  `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'hrsuper',
    lv_par,
    'Supervisor');
SET
  lv_par = "trim(coalesce(cast(Company as string),'')) ||\'-\'|| trim(coalesce(Super_Rpts_To,''))";
CALL
  `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'hrsuper',
    lv_par,
    'Supervisor');
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.supervisor_wrk;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.supervisor_wrk (supervisor_sid,
    eff_from_date,
    valid_from_date,
    employee_sid,
    employee_num,
    hr_company_sid,
    lawson_company_num,
    reporting_supervisor_sid,
    supervisor_code,
    supervisor_desc,
    process_level_code,
    eff_to_date,
    active_dw_ind,
    security_key_text,
    source_system_code,
    officer_code,
    dw_last_update_date_time)
SELECT
  CAST(xwlk.sk AS int64) AS supervisor_sid,
  hrs.effect_date AS eff_from_date,
  current_ts AS valid_from_date,
  CASE
    WHEN hrs.employee = 0 THEN 0
  ELSE
  emp.employee_sid
END
  AS employee_sid,
  hrs.employee AS employee_num,
  comp.hr_company_sid AS hr_company_sid,
  hrs.company AS lawson_company_num,
  CAST(lkp_rpt_supv_sid.sk AS int64) AS reporting_supervisor_sid,
  trim(hrs.code) AS supervisor_code,
  trim(hrs.description) AS supervisor_desc,
  '00000' AS process_level_code,
  date '9999-12-31' AS eff_to_date,
  'Y' AS active_dw_ind,
  CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(CAST(hrs.company AS string)))), TRIM(CAST(hrs.company AS string)), '-', '00000', '-', '00000') AS security_key_text,
  'L' AS source_system_code,
  trim(hrs.user1) AS officer_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.hrsuper AS hrs
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  UPPER(SUBSTR(CONCAT(TRIM(COALESCE(CAST(hrs.company AS string), '')), '-', TRIM(COALESCE(hrs.code, ''))), 1, 255)) = UPPER(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'SUPERVISOR'
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS lkp_rpt_supv_sid
ON
  UPPER(SUBSTR(CONCAT(TRIM(COALESCE(CAST(hrs.company AS string), '')), '-', TRIM(COALESCE(hrs.super_rpts_to, ''))), 1, 255)) = UPPER(lkp_rpt_supv_sid.sk_source_txt)
  AND UPPER(lkp_rpt_supv_sid.sk_type) = 'SUPERVISOR'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.hr_company AS comp
ON
  hrs.company = comp.lawson_company_num
  AND DATE(comp.valid_to_date) = '9999-12-31'
  AND UPPER(comp.source_system_code) = 'L'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.employee AS emp
ON
  hrs.employee = emp.employee_num
  AND hrs.company = emp.lawson_company_num
  AND DATE(emp.valid_to_date) = '9999-12-31'
  AND UPPER(emp.source_system_code) = 'L' ;

BEGIN TRANSACTION;
UPDATE
  {{ params.param_hr_core_dataset_name }}.supervisor AS tgt
SET
  valid_to_date = DATE_SUB(current_ts, INTERVAL 1 second),
  active_dw_ind = 'N'
WHERE
  date(tgt.valid_to_date) = '9999-12-31'
  AND tgt.source_system_code = 'L'
  AND (upper(TRIM(CAST(tgt.supervisor_sid AS string))),
    upper(TRIM(tgt.source_system_code))) NOT IN(
  SELECT
    AS STRUCT upper(TRIM(CAST(supervisor_wrk.supervisor_sid AS string))),
    upper(TRIM(supervisor_wrk.source_system_code))
  FROM
    {{ params.param_hr_stage_dataset_name }}.supervisor_wrk );




UPDATE
  {{ params.param_hr_core_dataset_name }}.supervisor AS supv
SET
  valid_to_date = DATE_SUB(current_ts, INTERVAL 1 second),
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.supervisor_wrk AS stg
WHERE
  UPPER(TRIM(COALESCE(CAST(supv.supervisor_sid AS string), ''))) = UPPER(TRIM(COALESCE(CAST(stg.supervisor_sid AS string), '')))
  AND supv.source_system_code = 'L'
  AND UPPER(TRIM(COALESCE(CAST(supv.hr_company_sid AS string), ''))) = UPPER(TRIM(COALESCE(CAST(stg.hr_company_sid AS string), '')))
  AND UPPER(TRIM(COALESCE(supv.source_system_code, ''))) = UPPER(TRIM(COALESCE(stg.source_system_code, '')))
  AND (UPPER(TRIM(COALESCE(CAST(supv.reporting_supervisor_sid AS string), ''))) <> UPPER(TRIM(COALESCE(CAST(stg.reporting_supervisor_sid AS string), '')))
    OR UPPER(TRIM(COALESCE(supv.supervisor_code, ''))) <> UPPER(TRIM(COALESCE(stg.supervisor_code, '')))
    OR UPPER(TRIM(COALESCE(supv.supervisor_desc, ''))) <> UPPER(TRIM(COALESCE(stg.supervisor_desc, '')))
    OR UPPER(TRIM(COALESCE(supv.officer_code, ''))) <> UPPER(TRIM(COALESCE(stg.officer_code, '')))
    OR UPPER(TRIM(COALESCE(CAST(supv.employee_sid AS string), ''))) <> UPPER(TRIM(COALESCE(CAST(stg.employee_sid AS string), '')))
    OR (supv.employee_num) <> (stg.employee_num))
  AND date(supv.valid_to_date) = '9999-12-31'
  AND supv.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.supervisor (supervisor_sid,
    eff_from_date,
    valid_from_date,
    hr_company_sid,
    employee_sid,
    employee_num,
    lawson_company_num,
    reporting_supervisor_sid,
    supervisor_code,
    supervisor_desc,
    process_level_code,
    valid_to_date,
    active_dw_ind,
    security_key_text,
    source_system_code,
    officer_code,
    dw_last_update_date_time)
SELECT
  supervisor_wrk.supervisor_sid,
  supervisor_wrk.eff_from_date,
  current_ts,
  supervisor_wrk.hr_company_sid,
  supervisor_wrk.employee_sid,
  supervisor_wrk.employee_num,
  supervisor_wrk.lawson_company_num,
  supervisor_wrk.reporting_supervisor_sid,
  supervisor_wrk.supervisor_code,
  supervisor_wrk.supervisor_desc,
  supervisor_wrk.process_level_code,
  DATETIME("9999-12-31 23:59:59"),
  supervisor_wrk.active_dw_ind,
  supervisor_wrk.security_key_text,
  supervisor_wrk.source_system_code,
  supervisor_wrk.officer_code,
  current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.supervisor_wrk
WHERE
  (UPPER(TRIM(COALESCE(CAST(supervisor_wrk.supervisor_sid AS string), ''))),
    UPPER(TRIM(COALESCE(CAST(supervisor_wrk.hr_company_sid AS string), ''))),
    UPPER(TRIM(COALESCE(CAST(supervisor_wrk.reporting_supervisor_sid AS string), ''))),
    UPPER(TRIM(COALESCE(supervisor_wrk.supervisor_code, ''))),
    UPPER(TRIM(COALESCE(supervisor_wrk.supervisor_desc, ''))),
    UPPER(TRIM(COALESCE(CAST(supervisor_wrk.employee_sid AS string),''))),
    UPPER(TRIM(COALESCE(CAST(supervisor_wrk.employee_num AS string),''))),
    UPPER(TRIM(COALESCE(supervisor_wrk.source_system_code, ''))),
    UPPER(TRIM(COALESCE(supervisor_wrk.officer_code, '')))) NOT IN(
  SELECT
    AS STRUCT UPPER(TRIM(COALESCE(CAST(supervisor.supervisor_sid AS string), ''))),
    UPPER(TRIM(COALESCE(CAST(supervisor.hr_company_sid AS string), ''))),
    UPPER(TRIM(COALESCE(CAST(supervisor.reporting_supervisor_sid AS string), ''))),
    UPPER(TRIM(COALESCE(supervisor.supervisor_code, ''))),
    UPPER(TRIM(COALESCE(supervisor.supervisor_desc, ''))),
    UPPER(TRIM(COALESCE(CAST(supervisor.employee_sid AS string),''))),
    UPPER(TRIM(COALESCE(CAST(supervisor.employee_num AS string),''))),
    UPPER(TRIM(COALESCE(supervisor.source_system_code, ''))),
    UPPER(TRIM(COALESCE(supervisor.officer_code, '')))
  FROM
    {{ params.param_hr_base_views_dataset_name }}.supervisor
  WHERE
    date(supervisor.valid_to_date) = '9999-12-31' AND supervisor.source_system_code = 'L') ;
SET
  dup_count = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      supervisor_sid,
      valid_from_date
    FROM
      {{ params.param_hr_core_dataset_name }}.supervisor
    GROUP BY
      supervisor_sid,
      valid_from_date
    HAVING
      COUNT(*) > 1 ) );
IF
  dup_count <> 0 THEN
ROLLBACK TRANSACTION; raise
USING
  message = CONCAT('duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.supervisor ');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;