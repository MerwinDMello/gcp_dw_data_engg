BEGIN
  DECLARE DUP_COUNT INT64;
  BEGIN TRANSACTION;

  /* Retire the records those are changed */
   /* Begin Transaction Block Starts Here */
UPDATE
  {{ params.param_hr_core_dataset_name }}.recruitment_position AS tgt
SET
  valid_to_date = CURRENT_DATETIME('US/Central') - INTERVAL 1 SECOND,
  dw_last_update_date_time = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
  FROM
  {{ params.param_hr_stage_dataset_name }}.recruitment_position_wrk AS wrk
WHERE
  wrk.recruitment_position_sid = tgt.recruitment_position_sid
  AND (COALESCE(cast(wrk.recruitment_position_num as string), CAST(123 AS STRING)) <> COALESCE(cast(tgt.recruitment_position_num as string), CAST(123 AS STRING))
    OR COALESCE(cast(wrk.gsd_pct as string), CAST(123 AS STRING)) <> COALESCE(cast(tgt.gsd_pct as string), CAST(123 AS STRING))
    OR COALESCE(cast(wrk.incentive_payout_pct as string), CAST(123 AS STRING)) <> COALESCE(cast(tgt.incentive_payout_pct as string), CAST(123 AS STRING))
    OR COALESCE(TRIM(wrk.incentive_plan_name), 'XXX') <> COALESCE(TRIM(tgt.incentive_plan_name), 'XXX')
    OR COALESCE(cast(wrk.incentive_plan_potential_pct as string), CAST(123 AS STRING)) <> COALESCE(cast(tgt.incentive_plan_potential_pct as string), CAST(123 AS STRING))
    OR COALESCE(TRIM(wrk.special_program_name), 'XXX') <> COALESCE(TRIM(tgt.special_program_name), 'XXX')
    OR COALESCE(TRIM(wrk.source_system_code), 'XXX') <> COALESCE(TRIM(tgt.source_system_code), 'XXX'))
  AND (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59");
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.recruitment_position (recruitment_position_sid,
    valid_from_date,
    valid_to_date,
    recruitment_position_num,
    gsd_pct,
    incentive_payout_pct,
    incentive_plan_name,
    incentive_plan_potential_pct,
    special_program_name,
    source_system_code,
    dw_last_update_date_time)
SELECT
  wrk.recruitment_position_sid,
  CURRENT_DATETIME('US/Central') AS valid_from_date,
  DATETIME("9999-12-31 23:59:59")  AS valid_to_date,
  wrk.recruitment_position_num,
  wrk.gsd_pct,
  wrk.incentive_payout_pct,
  wrk.incentive_plan_name,
  wrk.incentive_plan_potential_pct,
  wrk.special_program_name,
  wrk.source_system_code,
  TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.recruitment_position_wrk AS wrk
WHERE
  (wrk.recruitment_position_sid,
    wrk.recruitment_position_num,
    wrk.gsd_pct,
    wrk.incentive_payout_pct,
    wrk.incentive_plan_name,
    wrk.incentive_plan_potential_pct,
    wrk.special_program_name,
    wrk.source_system_code) NOT IN(
  SELECT
    AS STRUCT tgt.recruitment_position_sid,
    tgt.recruitment_position_num,
    tgt.gsd_pct,
    tgt.incentive_payout_pct,
    tgt.incentive_plan_name,
    tgt.incentive_plan_potential_pct,
    tgt.special_program_name,
    tgt.source_system_code
  FROM
    {{ params.param_hr_base_views_dataset_name }}.recruitment_position AS tgt
  WHERE
    (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59")); 

    /* End Transaction Block comment */ 
    /* Retire the records those are changed */ /* Begin Transaction Block Starts Here */
UPDATE
  {{ params.param_hr_core_dataset_name }}.recruitment_position AS tgt
SET
  valid_to_date = CURRENT_DATETIME('US/Central') - INTERVAL 1 SECOND,
  dw_last_update_date_time = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.recruitment_position_wrk AS wrk
WHERE
  wrk.recruitment_position_sid = tgt.recruitment_position_sid
  AND (COALESCE(CAST(wrk.recruitment_position_num AS STRING), CAST(123 AS STRING)) <> COALESCE(CAST(tgt.recruitment_position_num AS STRING), CAST(123 AS STRING))
    OR COALESCE(CAST(wrk.gsd_pct AS STRING), CAST(123 AS STRING)) <> COALESCE(CAST(tgt.gsd_pct AS STRING), CAST(123 AS STRING))
    OR COALESCE(CAST(wrk.incentive_payout_pct AS STRING), CAST(123 AS STRING)) <> COALESCE(CAST(tgt.incentive_payout_pct AS STRING), CAST(123 AS STRING))
    OR COALESCE(TRIM(wrk.incentive_plan_name), 'XXX') <> COALESCE(TRIM(tgt.incentive_plan_name), 'XXX')
    OR COALESCE(CAST(wrk.incentive_plan_potential_pct AS STRING), CAST(123 AS STRING)) <> COALESCE(CAST(tgt.incentive_plan_potential_pct AS STRING), CAST(123 AS STRING))
    OR COALESCE(TRIM(wrk.special_program_name), 'XXX') <> COALESCE(TRIM(tgt.special_program_name), 'XXX')
    OR COALESCE(TRIM(wrk.source_system_code), 'XXX') <> COALESCE(TRIM(tgt.source_system_code), 'XXX'))
  AND (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59");
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.recruitment_position (recruitment_position_sid,
    valid_from_date,
    valid_to_date,
    recruitment_position_num,
    gsd_pct,
    incentive_payout_pct,
    incentive_plan_name,
    incentive_plan_potential_pct,
    special_program_name,
    source_system_code,
    dw_last_update_date_time)
SELECT
  wrk.recruitment_position_sid,
  CURRENT_DATETIME('US/Central') AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  wrk.recruitment_position_num,
  wrk.gsd_pct,
  wrk.incentive_payout_pct,
  wrk.incentive_plan_name,
  wrk.incentive_plan_potential_pct,
  wrk.special_program_name,
  wrk.source_system_code,
  TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.recruitment_position_wrk AS wrk
WHERE
  (wrk.recruitment_position_sid,
    wrk.recruitment_position_num,
    wrk.gsd_pct,
    wrk.incentive_payout_pct,
    wrk.incentive_plan_name,
    wrk.incentive_plan_potential_pct,
    wrk.special_program_name,
    wrk.source_system_code) NOT IN(
  SELECT
    AS STRUCT tgt.recruitment_position_sid,
    tgt.recruitment_position_num,
    tgt.gsd_pct,
    tgt.incentive_payout_pct,
    tgt.incentive_plan_name,
    tgt.incentive_plan_potential_pct,
    tgt.special_program_name,
    tgt.source_system_code
  FROM
    {{ params.param_hr_base_views_dataset_name }}.recruitment_position AS tgt
  WHERE
    (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59") ) ;

  /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Recruitment_Position_SID ,Valid_From_Date
        from {{ params.param_hr_core_dataset_name }}.recruitment_position
        group by Recruitment_Position_SID ,Valid_From_Date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.recruitment_position');
    ELSE
      COMMIT TRANSACTION;
    END IF;


END;

