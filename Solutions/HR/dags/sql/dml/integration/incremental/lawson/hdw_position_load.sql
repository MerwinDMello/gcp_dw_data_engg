BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
CALL
  `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'paposition',
    "TRIM(cast(company as string))||'-'||TRIM(cast(position as string))",
    'POSITION');
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.job_position_wrk;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.job_position_wrk (position_sid,
    eff_from_date,
    valid_from_date,
    link_supervisor_sid,
    supervisor_sid,
    job_code_sid,
    hr_company_sid,
    position_change_reason_code,
    location_code,
    lawson_company_num,
    pay_grade_code,
    position_code,
    account_unit_num,
    gl_company_num,
    overtime_plan_code,
    position_code_desc,
    company_position_eff_from_date,
    company_position_eff_to_date,
    pay_grade_schedule_code,
    pay_step_num,
    union_code,
    shift_num,
    eff_to_date,
    valid_to_date,
    process_level_code,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time,
    exempt_emp,
    lawson_object_id,
    schedule_work_code,
    user_level_code,
    dept_sid)
SELECT
  cast(xwlk.sk as int64) AS position_sid,
  papos.effect_date AS eff_from_date,
  current_ts AS valid_from_date,
  COALESCE(sup_link.supervisor_sid, 0) AS link_supervisor_sid,
  COALESCE(sup.supervisor_sid, 0) AS supervisor_sid,
  COALESCE(jc.job_code_sid, 0) AS job_code_sid,
  hc.hr_company_sid AS hr_company_sid,
  TRIM(papos.chg_reason) AS position_change_reason_code,
  TRIM(papos.locat_code) AS location_code,
  papos.company AS lawson_company_num,
  papos.pay_grade AS pay_grade_code,
  papos.position AS position_code,
  TRIM(papos.exp_acct_unit) AS account_unit_num,
  papos.exp_company AS gl_company_num,
  TRIM(papos.ot_plan_code) AS overtime_plan_code,
  papos.description AS position_code_desc,
  papos.effect_date AS company_position_eff_from_date,
  papos.end_date AS company_position_eff_to_date,
  papos.r_schedule AS pay_grade_schedule_code,
  papos.pay_step AS pay_step_num,
  TRIM(papos.union_code) AS union_code,
  papos.shift AS shift_num,
  CASE
    WHEN papos.end_date = DATE '1800-01-01' THEN DATE '9999-12-31'
  ELSE
  papos.end_date
END
  AS eff_to_date,
  datetime("9999-12-31 23:59:59") AS valid_to_date,
  CASE
    WHEN UPPER(TRIM(COALESCE(papos.process_level, ''))) = '' THEN '00000'
  ELSE
  LPAD(TRIM(papos.process_level), 5, '0')
END
  AS process_level_code,
  'Y' AS active_dw_ind,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time,
  papos.exempt_emp,
  cast(papos.obj_id as int64) AS lawson_object_id,
  papos.work_sched AS schedule_work_code,
  papos.user_level AS user_level_code,
  dept.dept_sid AS dept_sid
FROM
  {{ params.param_hr_stage_dataset_name }}.paposition AS papos
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  UPPER(SUBSTR(CONCAT(TRIM(cast(papos.company as string)), '-', TRIM(cast(papos.position as string))), 1, 255)) = UPPER(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'POSITION'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.supervisor AS sup_link
ON
  sup_link.lawson_company_num = papos.company
  AND sup_link.supervisor_code = papos.supervisor_lnk
  AND date(sup_link.valid_to_date) = '9999-12-31'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.supervisor AS sup
ON
  sup.lawson_company_num = papos.company
  AND sup.supervisor_code = papos.supervisor
  AND date(sup.valid_to_date) = '9999-12-31'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.job_code AS jc
ON
  cast(jc.lawson_company_num as string) = TRIM(cast(papos.company as string))
  AND UPPER(jc.job_code) = UPPER(TRIM(COALESCE(papos.job_code, '')))
  AND UPPER(jc.source_system_code) = 'L'
  AND date(jc.valid_to_date) = '9999-12-31'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.hr_company AS hc
ON
  hc.lawson_company_num = papos.company
  AND date(hc.valid_to_date) = '9999-12-31'
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.department AS dept
ON
  UPPER(TRIM(COALESCE(papos.department, ''))) = UPPER(dept.dept_code)
  AND LPAD(TRIM(papos.process_level), 5, '0') = dept.process_level_code
  AND company = dept.lawson_company_num
  AND date(dept.valid_to_date) = '9999-12-31' ;

BEGIN TRANSACTION;
UPDATE
  {{ params.param_hr_core_dataset_name }}.job_position AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 second,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.job_position_wrk AS stg
WHERE
  tgt.position_sid = stg.position_sid
  AND tgt.eff_from_date = stg.eff_from_date
  AND (TRIM(cast(tgt.link_supervisor_sid as string)) <> TRIM(cast(stg.link_supervisor_sid as string))
    OR TRIM(cast(tgt.job_code_sid as string)) <> TRIM(cast(stg.job_code_sid as string))
    OR TRIM(cast(tgt.hr_company_sid as string)) <> TRIM(cast(stg.hr_company_sid as string))
    OR TRIM(cast(tgt.lawson_company_num as string)) <> TRIM(cast(stg.lawson_company_num as string))
    OR UPPER(TRIM(COALESCE(tgt.pay_grade_code, ''))) <> UPPER(TRIM(COALESCE(stg.pay_grade_code, '')))
    OR TRIM(tgt.position_code_desc) <> TRIM(stg.position_code_desc)
    OR UPPER(TRIM(COALESCE(tgt.location_code, ''))) <> UPPER(TRIM(COALESCE(stg.location_code, '')))
    OR (tgt.company_position_eff_from_date) <> (stg.company_position_eff_from_date)
    OR (tgt.company_position_eff_to_date) <> (stg.company_position_eff_to_date)
    OR UPPER(TRIM(COALESCE(tgt.pay_grade_schedule_code, ''))) <> UPPER(TRIM(COALESCE(stg.pay_grade_schedule_code, '')))
    OR UPPER(TRIM(COALESCE(cast(tgt.pay_step_num as string), ''))) <> UPPER(TRIM(COALESCE(cast(stg.pay_step_num as string), '')))
    OR TRIM(cast(tgt.supervisor_sid as string)) <> TRIM(cast(stg.supervisor_sid as string))
    OR UPPER(TRIM(COALESCE(tgt.account_unit_num, ''))) <> UPPER(TRIM(COALESCE(stg.account_unit_num, '')))
    OR TRIM(COALESCE(cast(tgt.gl_company_num as string), '')) <> TRIM(COALESCE(cast(stg.gl_company_num as string), ''))
    OR UPPER(TRIM(COALESCE(tgt.overtime_plan_code, ''))) <> UPPER(TRIM(COALESCE(stg.overtime_plan_code, '')))
    OR UPPER(TRIM(COALESCE(tgt.union_code, ''))) <> UPPER(TRIM(COALESCE(stg.union_code, '')))
    OR TRIM(COALESCE(cast(tgt.shift_num as string), '')) <> TRIM(COALESCE(cast(stg.shift_num as string), ''))
    OR TRIM(tgt.process_level_code) <> TRIM(stg.process_level_code)
    OR UPPER(TRIM(COALESCE(tgt.position_change_reason_code, ''))) <> UPPER(TRIM(COALESCE(stg.position_change_reason_code, '')))
    OR tgt.eff_to_date <> stg.eff_to_date
    OR UPPER(TRIM(COALESCE(tgt.overtime_exempt_ind, ''))) <> UPPER(TRIM(COALESCE(stg.exempt_emp, '')))
    OR UPPER(TRIM(COALESCE(cast(tgt.lawson_object_id as string), ''))) <> UPPER(TRIM(COALESCE(cast(stg.lawson_object_id as string), '')))
    OR UPPER(TRIM(COALESCE(tgt.source_system_code, ''))) <> UPPER(TRIM(COALESCE(stg.source_system_code, '')))
    OR UPPER(TRIM(COALESCE(tgt.schedule_work_code, ''))) <> UPPER(TRIM(COALESCE(stg.schedule_work_code, '')))
    OR UPPER(TRIM(COALESCE(tgt.user_level_code, ''))) <> UPPER(TRIM(COALESCE(stg.user_level_code, '')))
    OR UPPER(TRIM(COALESCE(cast(tgt.dept_sid as string), ''))) <> UPPER(TRIM(COALESCE(cast(stg.dept_sid as string), ''))))
  AND date(tgt.valid_to_date) = '9999-12-31'
  AND tgt.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.job_position AS jpos
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
WHERE
  UPPER(jpos.active_dw_ind) = 'Y'
  AND (TRIM(jpos.position_code),
    jpos.eff_from_date,
    jpos.lawson_company_num,
    UPPER(TRIM(COALESCE(jpos.source_system_code, '')))) NOT IN(
  SELECT
    AS STRUCT TRIM(position_code),
    job_position_wrk.eff_from_date,
    job_position_wrk.lawson_company_num,
    UPPER(TRIM(COALESCE(job_position_wrk.source_system_code, '')))
  FROM
    {{ params.param_hr_stage_dataset_name }}.job_position_wrk )
    AND jpos.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.job_position (position_sid,
    eff_from_date,
    valid_from_date,
    link_supervisor_sid,
    supervisor_sid,
    job_code_sid,
    hr_company_sid,
    position_change_reason_code,
    location_code,
    lawson_company_num,
    pay_grade_code,
    position_code,
    account_unit_num,
    gl_company_num,
    overtime_plan_code,
    position_code_desc,
    company_position_eff_from_date,
    company_position_eff_to_date,
    pay_grade_schedule_code,
    pay_step_num,
    union_code,
    shift_num,
    eff_to_date,
    valid_to_date,
    process_level_code,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time,
    overtime_exempt_ind,
    lawson_object_id,
    schedule_work_code,
    user_level_code,
    dept_sid)
SELECT
  job_position_wrk.position_sid,
  job_position_wrk.eff_from_date,
  current_ts,
  job_position_wrk.link_supervisor_sid,
  job_position_wrk.supervisor_sid,
  job_position_wrk.job_code_sid,
  job_position_wrk.hr_company_sid,
  job_position_wrk.position_change_reason_code,
  job_position_wrk.location_code,
  job_position_wrk.lawson_company_num,
  job_position_wrk.pay_grade_code,
  job_position_wrk.position_code,
  job_position_wrk.account_unit_num,
  job_position_wrk.gl_company_num,
  job_position_wrk.overtime_plan_code,
  job_position_wrk.position_code_desc,
  job_position_wrk.company_position_eff_from_date,
  job_position_wrk.company_position_eff_to_date,
  job_position_wrk.pay_grade_schedule_code,
  job_position_wrk.pay_step_num,
  job_position_wrk.union_code,
  job_position_wrk.shift_num,
  job_position_wrk.eff_to_date,
  datetime("9999-12-31 23:59:59"),
  job_position_wrk.process_level_code,
  job_position_wrk.active_dw_ind,
  job_position_wrk.source_system_code,
  job_position_wrk.dw_last_update_date_time,
  job_position_wrk.exempt_emp,
  job_position_wrk.lawson_object_id,
  job_position_wrk.schedule_work_code,
  job_position_wrk.user_level_code,
  job_position_wrk.dept_sid
FROM
  {{ params.param_hr_stage_dataset_name }}.job_position_wrk
WHERE
  (TRIM(cast(job_position_wrk.position_sid as string)),
    TRIM(cast(job_position_wrk.link_supervisor_sid as string)),
    TRIM(cast(job_position_wrk.job_code_sid as string)),
    TRIM(cast(job_position_wrk.hr_company_sid as string)),
    TRIM(cast(job_position_wrk.lawson_company_num as string)),
    UPPER(TRIM(COALESCE(job_position_wrk.pay_grade_code, ''))),
    TRIM(job_position_wrk.position_code),
    TRIM(job_position_wrk.position_code_desc),
    (job_position_wrk.company_position_eff_from_date),
    (job_position_wrk.company_position_eff_to_date),
    UPPER(TRIM(COALESCE(job_position_wrk.pay_grade_schedule_code, ''))),
    UPPER(TRIM(COALESCE(cast(job_position_wrk.pay_step_num as string), ''))),
    TRIM(cast(job_position_wrk.supervisor_sid as string)),
    UPPER(TRIM(COALESCE(job_position_wrk.account_unit_num, ''))),
    TRIM(COALESCE(cast(job_position_wrk.gl_company_num as string), '')),
    UPPER(TRIM(COALESCE(job_position_wrk.overtime_plan_code, ''))),
    UPPER(TRIM(COALESCE(job_position_wrk.union_code, ''))),
    TRIM(COALESCE(cast(job_position_wrk.shift_num as string), '')),
    TRIM(job_position_wrk.process_level_code),
    UPPER(TRIM(COALESCE(job_position_wrk.position_change_reason_code, ''))),
    UPPER(TRIM(COALESCE(job_position_wrk.location_code, ''))),
    job_position_wrk.eff_to_date,
    job_position_wrk.eff_from_date,
    UPPER(TRIM(COALESCE(job_position_wrk.exempt_emp, ''))),
    UPPER(TRIM(COALESCE(cast(job_position_wrk.lawson_object_id as string), ''))),
    UPPER(TRIM(COALESCE(job_position_wrk.source_system_code, ''))),
    UPPER(TRIM(COALESCE(job_position_wrk.schedule_work_code, ''))),
    UPPER(TRIM(COALESCE(job_position_wrk.user_level_code, ''))),
    COALESCE(job_position_wrk.dept_sid, -99)) NOT IN(
  SELECT
    AS STRUCT a.position_sid,
    a.link_supervisor_sid,
    a.job_code_sid,
    a.hr_company_sid,
    a.lawson_company_num,
    UPPER(a.pay_grade_code) AS pay_grade_code,
    a.position_code,
    a.position_code_desc,
    a.company_position_eff_from_date,
    a.company_position_eff_to_date,
    UPPER(a.pay_grade_schedule_code) AS pay_grade_schedule_code,
    UPPER(a.pay_step_num) AS pay_step_num,
    a.supervisor_sid,
    UPPER(a.account_unit_num) AS account_unit_num,
    a.gl_company_num,
    UPPER(a.overtime_plan_code) AS overtime_plan_code,
    UPPER(a.union_code) AS union_code,
    a.shift_num,
    a.process_level_code,
    UPPER(a.position_change_reason_code) AS position_change_reason_code,
    UPPER(a.location_code) AS location_code,
    a.eff_to_date,
    a.eff_from_date,
    UPPER(a.overtime_exempt_ind) AS overtime_exempt_ind,
    UPPER(a.lawson_object_id) AS lawson_object_id,
    UPPER(a.source_system_code) AS source_system_code,
    UPPER(a.schedule_work_code) AS schedule_work_code,
    UPPER(a.user_level_code) AS user_level_code,
    a.dept_sid
  FROM (
    SELECT
      TRIM(cast(job_position.position_sid as string)) AS position_sid,
      TRIM(cast(job_position.link_supervisor_sid as string)) AS link_supervisor_sid,
      TRIM(cast(job_position.job_code_sid as string)) AS job_code_sid,
      TRIM(cast(job_position.hr_company_sid as string)) AS hr_company_sid,
      TRIM(cast(job_position.lawson_company_num as string)) AS lawson_company_num,
      TRIM(COALESCE(job_position.pay_grade_code, '')) AS pay_grade_code,
      TRIM(job_position.position_code) AS position_code,
      TRIM(job_position.position_code_desc) AS position_code_desc,
      (job_position.company_position_eff_from_date) AS company_position_eff_from_date,
      (job_position.company_position_eff_to_date) AS company_position_eff_to_date,
      TRIM(COALESCE(job_position.pay_grade_schedule_code, '')) AS pay_grade_schedule_code,
      TRIM(COALESCE(cast(job_position.pay_step_num as string), '')) AS pay_step_num,
      TRIM(cast(job_position.supervisor_sid as string)) AS supervisor_sid,
      TRIM(COALESCE(job_position.account_unit_num, '')) AS account_unit_num,
      TRIM(COALESCE(cast(job_position.gl_company_num as string), '')) AS gl_company_num,
      TRIM(COALESCE(job_position.overtime_plan_code, '')) AS overtime_plan_code,
      TRIM(COALESCE(job_position.union_code, '')) AS union_code,
      TRIM(COALESCE(cast(job_position.shift_num as string), '')) AS shift_num,
      TRIM(job_position.process_level_code) AS process_level_code,
      TRIM(COALESCE(job_position.position_change_reason_code, '')) AS position_change_reason_code,
      TRIM(COALESCE(job_position.location_code, '')) AS location_code,
      job_position.eff_to_date,
      job_position.eff_from_date,
      TRIM(COALESCE(job_position.overtime_exempt_ind, '')) AS overtime_exempt_ind,
      TRIM(COALESCE(cast(job_position.lawson_object_id as string), '')) AS lawson_object_id,
      TRIM(COALESCE(job_position.source_system_code, '')) AS source_system_code,
      TRIM(COALESCE(job_position.schedule_work_code, '')) AS schedule_work_code,
      TRIM(COALESCE(job_position.user_level_code, '')) AS user_level_code,
      COALESCE(job_position.dept_sid, -99) AS dept_sid
    FROM
      {{ params.param_hr_core_dataset_name }}.job_position QUALIFY RANK() OVER (PARTITION BY job_position.position_sid ORDER BY job_position.valid_to_date DESC) = 1 ) AS a ) ;


SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Position_SID ,Eff_From_Date ,Valid_From_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.job_position
    GROUP BY
      Position_SID ,Eff_From_Date ,Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.job_position');
  ELSE
COMMIT TRANSACTION;
END IF
  ;

END
  ;