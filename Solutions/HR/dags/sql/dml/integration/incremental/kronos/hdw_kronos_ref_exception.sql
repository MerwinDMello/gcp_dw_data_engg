BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND) ;
BEGIN TRANSACTION;
MERGE INTO
  {{ params.param_hr_core_dataset_name }}.ref_exception AS tgt
USING
  (
  SELECT
    TRIM(rc.exception_code) AS exception_code,
    TRIM(rc.exception_desc) AS exception_desc,
    'K' AS source_system_code,
    current_ts AS dw_last_update_date_time
  FROM
    {{ params.param_hr_stage_dataset_name }}.ref_exception AS rc
  GROUP BY
    1,
    2,
    3,
    4 ) AS stg
ON
  tgt.exception_code = stg.exception_code
  WHEN MATCHED THEN UPDATE SET exception_desc = stg.exception_desc, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
  WHEN NOT MATCHED BY TARGET
  THEN
INSERT
  (exception_code,
    exception_desc,
    source_system_code,
    dw_last_update_date_time)
VALUES
  (stg.exception_code, stg.exception_desc, stg.source_system_code, stg.dw_last_update_date_time) ; /* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      exception_code
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_exception
    GROUP BY
      exception_code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: edwhr.ref_exception');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;