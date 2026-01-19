DECLARE current_dt datetime;
SET current_dt = datetime_trunc(current_datetime('US/Central'), SECOND);

CALL `{{ params.param_hr_core_dataset_name }}.sk_gen`("{{ params.param_hr_stage_dataset_name }}","development_activities_report","Trim(development_activity_record_id)","Employee_Development_Activity");

  -- NO target-dialect support FOR source-dialect-specific 
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.development_activities_report_reject;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.development_activities_report_reject
SELECT
  stg.employee_id,
  stg.activity_competency_name,
  stg.description,
  stg.catalog_activity_name,
  stg.catalog_activity_description,
  stg.priority,
  stg.status,
  stg.start_date,
  stg.end_date,
  stg.hours,
  stg.development_goals_notes,
  stg.development_activity_record_id,
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
  'DEVELOPMENT_ACTIVITIES_REPORT' AS reject_stg_tbl_nm
FROM
  {{ params.param_hr_stage_dataset_name }}.development_activities_report AS stg
WHERE
  SUBSTR(TRIM(stg.employee_id), 1, 1) NOT IN( '1','2','3','4','5','6','7','8','9')
  OR ( SUBSTR(TRIM(stg.employee_id), 1, 1) IN( '1','2','3','4','5','6','7','8','9')
       AND CASE COALESCE(TRIM(stg.employee_id),'')
            WHEN '' THEN 0
            ELSE CAST(TRIM(stg.employee_id) AS INT64)
           END
       NOT IN (SELECT employee_num FROM {{ params.param_hr_core_dataset_name }}.employee)) ;

TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.employee_development_activity_wrk;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.employee_development_activity_wrk (employee_development_activity_sid,
    employee_num,
    employee_sid,
    employee_talent_profile_sid,
    development_activity_name,
    development_activity_desc,
    catalog_activity_name,
    catalog_activity_desc,
    development_activity_status_id,
    development_activity_priority_id,
    development_activity_start_date,
    development_activity_end_date,
    development_activity_hour_text,
    development_activity_comment_text,
    lawson_company_num,
    process_level_code,
    source_system_key,
    source_system_code,
    dw_last_update_date_time)
SELECT
  CAST(xwlk.sk AS int64) AS employee_development_activity_sid,
  CAST(stg.employee_id AS int64) AS employee_num,
  emp.employee_sid AS employee_sid,
  etp.employee_talent_profile_sid AS employee_talent_profile_sid,
  stg.activity_competency_name AS activity_competency_name,
  stg.description AS description,
  stg.catalog_activity_name AS catalog_activity_name,
  stg.catalog_activity_description AS catalog_activity_description,
  rps.performance_status_id AS performance_status_id,
  rpp.probability_potential_id AS probability_potential_id,
  stg.start_date AS start_date,
  stg.end_date AS end_date,
  stg.hours AS hours,
  stg.development_goals_notes AS development_goals_notes,
  etp.lawson_company_num AS lawson_company_num,
  COALESCE(etp.process_level_code, '00000') AS process_level_code,
  stg.development_activity_record_id AS development_activity_record_id,
  'M' AS source_system_code,
  datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.development_activities_report AS stg
INNER JOIN
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
ON
  TRIM(stg.development_activity_record_id) = xwlk.sk_source_txt
  AND UPPER(xwlk.sk_type) = 'EMPLOYEE_DEVELOPMENT_ACTIVITY'
INNER JOIN
  {{ params.param_hr_core_dataset_name }}.employee AS emp
ON
  CASE
    WHEN TRIM(stg.employee_id) IS NOT NULL 
      AND SUBSTR(TRIM(stg.employee_id), 1, 1) IN( '1', '2', '3', '4', '5', '6', '7', '8', '9' ) 
    THEN CAST(TRIM(stg.employee_id) AS INT64)
    ELSE 0
  END = emp.employee_num
  AND (emp.valid_to_date) = DATETIME("9999-12-31 23:59:59") 
  AND emp.lawson_company_num = CASE SUBSTR(stg.job_code, 1, 4)
                                WHEN '' THEN 0
                               ELSE CAST(SUBSTR(stg.job_code, 1, 4) AS INT64)
                               END
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.employee_talent_profile AS etp
ON
  CASE
    WHEN TRIM(stg.employee_id) IS NOT NULL 
        AND SUBSTR(TRIM(stg.employee_id), 1, 1) IN( '1', '2', '3', '4', '5', '6', '7', '8', '9' ) 
    THEN CAST(TRIM(stg.employee_id) AS INT64)
    ELSE 0
  END = etp.employee_num
  AND (etp.valid_to_date) = DATETIME("9999-12-31 23:59:59") 
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.ref_performance_status AS rps
ON
  rps.performance_status_desc = stg.status
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.ref_probability_potential AS rpp
ON
  rpp.probability_potential_desc = stg.priority QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_development_activity_sid ORDER BY employee_development_activity_sid) = 1 ;

UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_development_activity AS tgt
SET
  valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND),
  dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND)
FROM (
  SELECT
    employee_development_activity_wrk.*
  FROM
    {{ params.param_hr_stage_dataset_name }}.employee_development_activity_wrk ) AS wrk
WHERE
  (tgt.employee_development_activity_sid) = (wrk.employee_development_activity_sid)
  AND (COALESCE(tgt.employee_talent_profile_sid, 0) <> COALESCE(wrk.employee_talent_profile_sid, 0)
    OR UPPER(TRIM(COALESCE(tgt.development_activity_name, ''))) <> UPPER(TRIM(COALESCE(wrk.development_activity_name, '')))
    OR COALESCE(tgt.employee_num, 0) <> COALESCE(wrk.employee_num, 0)
    OR COALESCE(tgt.employee_sid, 0) <> COALESCE(wrk.employee_sid, 0)
    OR UPPER(TRIM(COALESCE(tgt.development_activity_desc, ''))) <> UPPER(TRIM(COALESCE(wrk.development_activity_desc, '')))
    OR UPPER(TRIM(COALESCE(tgt.catalog_activity_name, ''))) <> UPPER(TRIM(COALESCE(wrk.catalog_activity_name, '')))
    OR UPPER(TRIM(COALESCE(tgt.catalog_activity_desc, ''))) <> UPPER(TRIM(COALESCE(wrk.catalog_activity_desc, '')))
    OR COALESCE(tgt.development_activity_status_id, 0) <> COALESCE(wrk.development_activity_status_id, 0)
    OR COALESCE(tgt.development_activity_priority_id, 0) <> COALESCE(wrk.development_activity_priority_id, 0)
    OR COALESCE(tgt.development_activity_start_date, DATE '9999-01-01') <> COALESCE(wrk.development_activity_start_date, DATE '9999-01-01')
    OR COALESCE(tgt.development_activity_end_date, DATE '9999-01-01') <> COALESCE(wrk.development_activity_end_date, DATE '9999-01-01')
    OR UPPER(TRIM(COALESCE(tgt.development_activity_hour_text, ''))) <> UPPER(TRIM(COALESCE(wrk.development_activity_hour_text, '')))
    OR UPPER(TRIM(COALESCE(tgt.development_activity_comment_text, ''))) <> UPPER(TRIM(COALESCE(wrk.development_activity_comment_text, '')))
    OR COALESCE(tgt.lawson_company_num, 0) <> COALESCE(wrk.lawson_company_num, 0)
    OR UPPER(TRIM(COALESCE(tgt.process_level_code, ''))) <> UPPER(TRIM(COALESCE(wrk.process_level_code, '')))
    OR UPPER(TRIM(COALESCE(tgt.source_system_key, ''))) <> UPPER(TRIM(COALESCE(wrk.source_system_key, ''))))
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59") ;

BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.employee_development_activity (employee_development_activity_sid,
    valid_from_date,
    employee_sid,
    employee_num,
    employee_talent_profile_sid,
    development_activity_name,
    development_activity_desc,
    catalog_activity_name,
    catalog_activity_desc,
    development_activity_status_id,
    development_activity_priority_id,
    development_activity_start_date,
    development_activity_end_date,
    development_activity_hour_text,
    development_activity_comment_text,
    lawson_company_num,
    process_level_code,
    valid_to_date,
    source_system_key,
    source_system_code,
    dw_last_update_date_time)
SELECT
  wrk.employee_development_activity_sid,
  current_dt AS valid_from_date,
  wrk.employee_sid,
  wrk.employee_num,
  wrk.employee_talent_profile_sid,
  Trim(wrk.development_activity_name),
  Trim(wrk.development_activity_desc),
  Trim(wrk.catalog_activity_name),
  Trim(wrk.catalog_activity_desc),
  wrk.development_activity_status_id,
  wrk.development_activity_priority_id,
  wrk.development_activity_start_date,
  wrk.development_activity_end_date,
  Trim(wrk.development_activity_hour_text),
  Trim(wrk.development_activity_comment_text),
  wrk.lawson_company_num,
  wrk.process_level_code,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date,
  wrk.source_system_key,
  wrk.source_system_code,
  datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_development_activity_wrk AS wrk
WHERE
  ( COALESCE(wrk.employee_development_activity_sid,0),
    COALESCE(wrk.employee_sid,0),
    COALESCE(wrk.employee_num,0),
    COALESCE(wrk.employee_talent_profile_sid,0),
    UPPER(COALESCE(Trim(wrk.development_activity_name),'')),
    UPPER(COALESCE(Trim(wrk.development_activity_desc),'')),
    UPPER(COALESCE(Trim(wrk.catalog_activity_name),'')),
    UPPER(COALESCE(Trim(wrk.catalog_activity_desc),'')),
    COALESCE(wrk.development_activity_status_id,0),
    COALESCE(wrk.development_activity_priority_id,0),
    COALESCE(wrk.development_activity_start_date,DATE '9999-01-01'),
    COALESCE(wrk.development_activity_end_date,DATE '9999-01-01'),
    UPPER(COALESCE(Trim(wrk.development_activity_hour_text),'')),
    UPPER(COALESCE(Trim(wrk.development_activity_comment_text),'')),
    COALESCE(wrk.lawson_company_num,0),
    UPPER(COALESCE(Trim(wrk.process_level_code),'')),
    UPPER(COALESCE(Trim(wrk.source_system_key),'')),
    UPPER(COALESCE(Trim(wrk.source_system_code),'M'))) NOT IN(
  SELECT
    AS STRUCT COALESCE(tgt.employee_development_activity_sid,0),
    COALESCE(tgt.employee_sid,0),
    COALESCE(tgt.employee_num,0),
    COALESCE(tgt.employee_talent_profile_sid,0),
    UPPER(COALESCE(Trim(tgt.development_activity_name),'')),
    UPPER(COALESCE(Trim(tgt.development_activity_desc),'')),
    UPPER(COALESCE(Trim(tgt.catalog_activity_name),'')),
    UPPER(COALESCE(Trim(tgt.catalog_activity_desc),'')),
    COALESCE(tgt.development_activity_status_id,0),
    COALESCE(tgt.development_activity_priority_id,0),
    COALESCE(tgt.development_activity_start_date, DATE '9999-01-01'),
    COALESCE(tgt.development_activity_end_date,DATE '9999-01-01'),
    UPPER(COALESCE(Trim(tgt.development_activity_hour_text),'')),
    UPPER(COALESCE(Trim(tgt.development_activity_comment_text),'')),
    COALESCE(tgt.lawson_company_num,0),
    UPPER(COALESCE(Trim(tgt.process_level_code),'')),
    UPPER(COALESCE(Trim(tgt.source_system_key),'')),
    UPPER(COALESCE(Trim(tgt.source_system_code),'M'))
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_development_activity AS tgt
  WHERE
    (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59")  ) ;


  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT employee_development_activity_sid ,valid_from_date 
      FROM {{ params.param_hr_core_dataset_name }}.employee_development_activity
      GROUP BY employee_development_activity_sid ,valid_from_date 
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.employee_development_activity');
  END IF;

UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_development_activity AS tgt
SET
  valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND),
  dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND)
WHERE
  tgt.valid_to_date = DATETIME("9999-12-31 23:59:59") 
  AND tgt.employee_development_activity_sid NOT IN(
  SELECT
    DISTINCT wrk.employee_development_activity_sid
  FROM
    {{ params.param_hr_stage_dataset_name }}.employee_development_activity_wrk AS wrk );
    COMMIT TRANSACTION;
END
;
