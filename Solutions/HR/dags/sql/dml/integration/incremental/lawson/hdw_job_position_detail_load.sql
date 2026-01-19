BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE
  current_ts datetime;
SET
  current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) ;
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.job_position_detail_wrk;
BEGIN TRANSACTION;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.job_position_detail_wrk (position_sid,
    position_type_id,
    position_detail_code,
    eff_from_date,
    valid_from_date,
    valid_to_date,
    eff_to_date,
    detail_value_alphanumeric_text,
    detail_value_num,
    detail_value_date,
    lawson_object_id,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time)
SELECT
  COALESCE(j.position_sid, 0) AS position_sid,
  TRIM(h.hrpousd_type) AS position_type_id,
  TRIM(h.field_key) AS position_detail_code,
  h.effect_date AS eff_from_date,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  h.end_date AS eff_to_date,
  TRIM(h.a_field) AS detail_value_alphanumeric_text,
  CAST(TRUNC(h.n_field) AS INT64) AS detail_value_num,
  h.d_field AS detail_value_date,
  CAST(h.obj_id AS INT64) AS lawson_object_id,
  h.company AS lawson_company_num,
  '00000' AS process_level_code,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.hrposusd AS h
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.job_position AS j
ON
  h.obj_id = j.lawson_object_id
  AND h.company = j.lawson_company_num
  AND h.effect_date = j.eff_from_date
  AND UPPER(j.source_system_code) = 'L'
  AND DATE(j.valid_to_date) = "9999-12-31"; /* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Position_SID,
      Position_Type_Id,
      Position_Detail_Code,
      Eff_From_Date,
      Valid_From_Date
    FROM
      {{ params.param_hr_stage_dataset_name }}.job_position_detail_wrk
    GROUP BY
      Position_SID,
      Position_Type_Id,
      Position_Detail_Code,
      Eff_From_Date,
      Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_stage_dataset_name }}.job_position_detail_wrk');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
BEGIN TRANSACTION;
UPDATE
  {{ params.param_hr_core_dataset_name }}.job_position_detail AS tgt
SET
  valid_to_date = DATE_ADD(current_ts, INTERVAL -1 second),
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.job_position_detail_wrk AS stg
WHERE
  tgt.position_sid = stg.position_sid
  AND tgt.source_system_code = 'L'
  AND UPPER(TRIM(tgt.position_type_id)) = UPPER(TRIM(stg.position_type_id))
  AND UPPER(TRIM(tgt.position_detail_code)) = UPPER(TRIM(stg.position_detail_code))
  AND tgt.eff_from_date = stg.eff_from_date
  AND (tgt.eff_to_date <> stg.eff_to_date
    OR UPPER(TRIM(COALESCE(TRIM(tgt.detail_value_alphanumeric_text),'X'))) <> UPPER(TRIM(COALESCE(TRIM(stg.detail_value_alphanumeric_text),'X')))
    OR tgt.detail_value_num <> stg.detail_value_num
    OR tgt.detail_value_date <> stg.detail_value_date
    OR tgt.lawson_object_id <> stg.lawson_object_id
    OR tgt.lawson_company_num <> stg.lawson_company_num
    OR UPPER(TRIM( tgt.process_level_code)) <> UPPER(TRIM(stg.process_level_code)));
UPDATE
  {{ params.param_hr_core_dataset_name }}.job_position_detail AS tgt
SET
  valid_to_date = DATE_ADD( current_ts, INTERVAL -1 second),
  dw_last_update_date_time = current_ts
WHERE
  DATE(tgt.valid_to_date) = "9999-12-31"
  AND tgt.source_system_code = 'L'
  AND (tgt.position_sid,
    UPPER(TRIM(tgt.position_type_id)),
    UPPER(TRIM(tgt.position_detail_code)),
    tgt.eff_from_date,
    DATE(tgt.valid_from_date)) NOT IN(
  SELECT
    AS STRUCT job_position_detail_wrk.position_sid,
    UPPER(TRIM(job_position_detail_wrk.position_type_id)),
    UPPER(TRIM(job_position_detail_wrk.position_detail_code)),
    job_position_detail_wrk.eff_from_date,
    DATE(job_position_detail_wrk.valid_from_date)
  FROM
    {{ params.param_hr_stage_dataset_name }}.job_position_detail_wrk );
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.job_position_detail (position_sid,
    position_type_id,
    position_detail_code,
    eff_from_date,
    valid_from_date,
    valid_to_date,
    eff_to_date,
    detail_value_alphanumeric_text,
    detail_value_num,
    detail_value_date,
    lawson_object_id,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time)
SELECT
  job_position_detail_wrk.position_sid,
  job_position_detail_wrk.position_type_id,
  job_position_detail_wrk.position_detail_code,
  job_position_detail_wrk.eff_from_date,
  current_ts,
  job_position_detail_wrk.valid_to_date,
  job_position_detail_wrk.eff_to_date,
  job_position_detail_wrk.detail_value_alphanumeric_text,
  job_position_detail_wrk.detail_value_num,
  job_position_detail_wrk.detail_value_date,
  job_position_detail_wrk.lawson_object_id,
  job_position_detail_wrk.lawson_company_num,
  job_position_detail_wrk.process_level_code,
  job_position_detail_wrk.source_system_code,
  current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.job_position_detail_wrk
WHERE
  (job_position_detail_wrk.position_sid,
    UPPER(TRIM(job_position_detail_wrk.position_type_id)),
    UPPER(TRIM(job_position_detail_wrk.position_detail_code)),
    job_position_detail_wrk.eff_from_date) NOT IN(
  SELECT
    AS STRUCT job_position_detail.position_sid,
    UPPER(TRIM(job_position_detail.position_type_id)),
    UPPER(TRIM(job_position_detail.position_detail_code)),
    job_position_detail.eff_from_date
  FROM
    {{ params.param_hr_base_views_dataset_name }}.job_position_detail
  WHERE
    DATE(job_position_detail.valid_to_date) = "9999-12-31"
    AND job_position_detail.source_system_code = 'L') ;
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Position_SID,
      Position_Type_Id,
      Position_Detail_Code,
      Eff_From_Date,
      Valid_From_Date
    FROM
      {{ params.param_hr_base_views_dataset_name }}.job_position_detail
    GROUP BY
      Position_SID,
      Position_Type_Id,
      Position_Detail_Code,
      Eff_From_Date,
      Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_base_views_dataset_name }}.job_position_detail');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;