BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE current_dt DATETIME;
SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
  BEGIN TRANSACTION;
  
  
  /*  Insert the New Records/Chnages into the Target Table  */ 
  /* Begin Transaction Block Starts Here */ /*  RETIRE RECORD ON 2ND RETIRE LOGIC */
UPDATE
  {{ params.param_hr_core_dataset_name }}.junc_candidate_communication_device AS tgt
SET
  valid_to_date = current_dt - INTERVAL 1 SECOND,
  dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.junc_candidate_communication_device_wrk AS stg
WHERE
  tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND tgt.candidate_sid = stg.candidate_sid
  AND tgt.source_system_code = stg.source_system_code
  AND TRIM(tgt.communication_device_type_code) = TRIM(stg.communication_device_type_code)
  AND tgt.communication_device_sid <> stg.communication_device_sid;
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.junc_candidate_communication_device (communication_device_sid,
    candidate_sid,
    communication_device_type_code,
    valid_from_date,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.communication_device_sid,
  stg.candidate_sid,
  stg.communication_device_type_code,
  current_dt,
  DATETIME("9999-12-31 23:59:59"),
  stg.source_system_code,
  stg.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.junc_candidate_communication_device_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.junc_candidate_communication_device AS tgt
ON
  stg.communication_device_sid = tgt.communication_device_sid
  AND stg.candidate_sid = tgt.candidate_sid
  AND tgt.source_system_code = stg.source_system_code
  AND stg.communication_device_type_code = tgt.communication_device_type_code
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.candidate_sid IS NULL ;


/* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT      
  communication_device_sid,candidate_sid ,communication_device_type_code ,valid_from_date
    FROM
      {{ params.param_hr_core_dataset_name }}.junc_candidate_communication_device
    GROUP BY      
  communication_device_sid,candidate_sid ,communication_device_type_code ,valid_from_date
     HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: candidate');
  ELSE
COMMIT TRANSACTION;
END IF;
END  ;