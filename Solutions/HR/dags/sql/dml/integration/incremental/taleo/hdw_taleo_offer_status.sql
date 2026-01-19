BEGIN
DECLARE DUP_COUNT INT64;
DECLARE  current_dt DATETIME;
SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
BEGIN TRANSACTION; 
/*  Close the previous records from Target table for same key for any Changes  */ 
/*  Insert the New Records/Chnages into the Target Table  */
 /* Begin Transaction Block Starts Here */
UPDATE
  {{ params.param_hr_core_dataset_name }}.offer_status AS tgt
SET
  valid_to_date = current_dt - INTERVAL 1 second,
  dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.offer_status_wrk AS stg
WHERE
  tgt.offer_sid = stg.offer_sid
  AND (TRIM(CAST(COALESCE(tgt.offer_status_id, -999) AS STRING)) <> TRIM(CAST(COALESCE(stg.offer_status_id, -999) AS STRING))
    OR COALESCE(TRIM(tgt.source_system_code), 'X') <> COALESCE(TRIM(stg.source_system_code), 'X'))
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.offer_status (offer_sid,
    valid_from_date,
    offer_status_id,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.offer_sid,
  current_dt as valid_from_date,
  stg.offer_status_id,
  DATETIME("9999-12-31 23:59:59") as valid_to_date,
  stg.source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.offer_status_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.offer_status AS tgt
ON
  stg.offer_sid = tgt.offer_sid
  AND TRIM(CAST(COALESCE(tgt.offer_status_id, -999) AS STRING)) = TRIM(CAST(COALESCE(stg.offer_status_id, -999) AS STRING))
  AND COALESCE(TRIM(tgt.source_system_code), 'X') = COALESCE(TRIM(stg.source_system_code), 'X')
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.offer_sid IS NULL ; /* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      offer_sid,valid_from_date
    FROM
      {{ params.param_hr_core_dataset_name }}.offer_status
    GROUP BY
      offer_sid,valid_from_date      
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.offer_status');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;