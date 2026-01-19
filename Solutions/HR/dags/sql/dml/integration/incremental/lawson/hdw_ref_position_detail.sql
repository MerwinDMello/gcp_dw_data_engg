BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = current_datetime('US/Central');

BEGIN TRANSACTION;
DELETE FROM
  {{ params.param_hr_core_dataset_name }}.ref_position_detail WHERE 1 = 1;
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.ref_position_detail (position_detail_code,
    position_detail_code_desc,
    source_system_code,
    dw_last_update_date_time)
SELECT
  trim(hrposflds.field_key) AS position_detail_code,
  trim(hrposflds.field_name) AS position_detail_code_desc,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.hrposflds ;
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Position_Detail_Code
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_position_detail
    GROUP BY
      Position_Detail_Code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_position_detail');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;
