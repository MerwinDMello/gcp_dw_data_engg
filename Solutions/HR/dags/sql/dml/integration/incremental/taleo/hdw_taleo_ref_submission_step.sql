BEGIN
DECLARE
  DUP_COUNT INT64;
BEGIN TRANSACTION; 
/*  LOAD WORK TABLE WITH WORKING DATA */
MERGE INTO
  {{ params.param_hr_core_dataset_name }}.ref_submission_step AS tgt
USING
  (
  SELECT
    number AS step_id,
    available AS submission_state_id,
    applicationstate_number AS active_sw,
    TRIM(mnemonic) AS step_code,
    TRIM(name) AS step_name,
    TRIM(shortname) AS step_short_name,
    TRIM(description) AS step_desc,
    'T' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
  FROM
    {{ params.param_hr_stage_dataset_name }}.taleo_cswstep
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9 ) AS stg
ON
  tgt.step_id = stg.step_id
  WHEN MATCHED THEN UPDATE SET submission_state_id = stg.submission_state_id, active_sw = stg.active_sw, step_code = stg.step_code, step_name = stg.step_name, step_short_name = stg.step_short_name, step_desc = stg.step_desc, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
  WHEN NOT MATCHED BY TARGET
  THEN
INSERT
  (step_id,
    submission_state_id,
    active_sw,
    step_code,
    step_name,
    step_short_name,
    step_desc,
    source_system_code,
    dw_last_update_date_time)
VALUES
  (stg.step_id, stg.submission_state_id, stg.active_sw, stg.step_code, stg.step_name, stg.step_short_name, stg.step_desc, stg.source_system_code, stg.dw_last_update_date_time) ; 
  -- ATS MERGE  LOGIC TO GENERATE ID --HDM-1456
MERGE INTO
  {{ params.param_hr_core_dataset_name }}.ref_submission_step AS tgt
USING
  (
  SELECT
    DISTINCT
    CASE
      WHEN tgt_0.step_id IS NOT NULL THEN tgt_0.step_id
    ELSE
    (
    SELECT
      COALESCE(MAX(step_id), CAST(1000000 AS BIGNUMERIC))
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_submission_step
    WHERE
      UPPER(stgg.source_system_code) = 'B' ) + CAST(ROW_NUMBER() OVER (ORDER BY tgt_0.step_id, stgg.step_code) AS BIGNUMERIC)
  END
    AS step_id,
    stgg.submission_state_id,
    stgg.active_sw,
    stgg.step_code,
    stgg.step_name,
    stgg.step_short_name,
    stgg.step_desc,
    stgg.source_system_code,
    stgg.dw_last_update_date_time
  FROM (
    SELECT
      DISTINCT '0' AS submission_state_id,
      wc.active AS active_sw,
      TRIM(wc.atsworkflowcategorykey)  AS step_code,
      TRIM(wc.atsworkflowcategorykey)  AS step_name,
      TRIM(wc.description) AS step_short_name,
      TRIM(wc.description) AS step_desc,
      'B' AS source_system_code,
      DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
    FROM
      {{ params.param_hr_stage_dataset_name }}.ats_hcm_atsworkflowcategory_stg AS wc ) AS stgg
  LEFT OUTER JOIN
    {{ params.param_hr_base_views_dataset_name }}.ref_submission_step AS tgt_0
  ON
    TRIM(tgt_0.step_code) = TRIM(stgg.step_code)
    AND UPPER(tgt_0.source_system_code) = 'B' ) AS stg
ON
  tgt.step_id = stg.step_id
  WHEN MATCHED THEN UPDATE SET submission_state_id = CAST(stg.submission_state_id AS int64), active_sw = CAST(stg.active_sw AS int64), step_code = stg.step_code, step_name = stg.step_name, step_short_name = stg.step_short_name, step_desc = stg.step_desc, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
  WHEN NOT MATCHED BY TARGET
  THEN
INSERT
  (step_id,
    submission_state_id,
    active_sw,
    step_code,
    step_name,
    step_short_name,
    step_desc,
    source_system_code,
    dw_last_update_date_time)
VALUES
  (CAST(stg.step_id AS int64), CAST(stg.submission_state_id AS int64), CAST(stg.active_sw AS int64), stg.step_code, stg.step_name, stg.step_short_name, stg.step_desc, stg.source_system_code, stg.dw_last_update_date_time) ; /* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Step_Id
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_submission_step
    GROUP BY
      Step_Id
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_submission_step');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;