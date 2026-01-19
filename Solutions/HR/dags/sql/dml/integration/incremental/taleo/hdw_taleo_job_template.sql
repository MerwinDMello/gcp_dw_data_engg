BEGIN
DECLARE  current_dt DATETIME;
DECLARE  DUP_COUNT INT64;
SET   current_dt = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND); --DATETIME("9999-12-31 23:59:59+00")

BEGIN TRANSACTION;

UPDATE
  {{ params.param_hr_core_dataset_name }}.job_template AS tgt
SET valid_to_date = current_dt - INTERVAL 1 second,
  dw_last_update_date_time = TIMESTAMP_TRUNC(CURRENT_DATETIME(), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.job_template_wrk AS stg
WHERE
  tgt.job_template_sid = stg.job_template_sid
  AND (TRIM(CAST(COALESCE(tgt.job_template_num, -999) AS STRING)) <> TRIM(CAST(COALESCE(stg.job_template_num, -999) AS STRING))
    OR TRIM(CAST(COALESCE(tgt.base_job_template_num, -9) AS STRING)) <> TRIM(CAST(COALESCE(stg.base_job_template_num, -9) AS STRING))
    OR TRIM(CAST(COALESCE(tgt.recruitment_job_sid, -9) AS STRING)) <> TRIM(CAST(COALESCE(stg.recruitment_job_sid, -9) AS STRING))
    OR TRIM(CAST(COALESCE(tgt.job_template_status_id, -9) AS STRING)) <> TRIM(CAST(COALESCE(stg.job_template_status_id, -9) AS STRING)))
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.job_template (
  job_template_sid,
    valid_from_date,
    job_template_num,
    base_job_template_num,
    recruitment_job_sid,
    job_template_status_id,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.job_template_sid,
  current_dt,
  stg.job_template_num,
  stg.base_job_template_num,
  stg.recruitment_job_sid,
  stg.job_template_status_id,
  DATETIME("9999-12-31 23:59:59"),
  stg.source_system_code,
  current_dt
FROM
  {{ params.param_hr_stage_dataset_name }}.job_template_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.job_template AS tgt
ON
  stg.job_template_sid = tgt.job_template_sid
  AND TRIM(CAST(COALESCE(tgt.job_template_num, -999) AS STRING)) = TRIM(CAST(COALESCE(stg.job_template_num, -999) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.base_job_template_num, -9) AS STRING)) = TRIM(CAST(COALESCE(stg.base_job_template_num, -9) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.recruitment_job_sid, -9) AS STRING)) = TRIM(CAST(COALESCE(stg.recruitment_job_sid, -9) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.job_template_status_id, -9) AS STRING)) = TRIM(CAST(COALESCE(stg.job_template_status_id, -9) AS STRING))
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.job_template_sid IS NULL ;

SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       job_template_sid,valid_from_date
    FROM
      {{ params.param_hr_core_dataset_name }}.job_template
    GROUP BY
       job_template_sid,valid_from_date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.job_template');
  ELSE
COMMIT TRANSACTION;
END IF
;
END