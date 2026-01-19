
DECLARE ts datetime;
DECLARE DUP_COUNT INT64;
set ts = datetime_trunc(current_datetime('US/Central'), Second);

BEGIN

CREATE TEMP TABLE T1 AS(
SELECT DISTINCT src1.action_type, 
CASE WHEN (src1.action_type = '' or src1.action_type is null) 
then 'Unknown'
else src2.action_type_desc
end as action_type_desc
FROM (SELECT distinct action_type from {{ params.param_hr_stage_dataset_name }}.persaction
UNION ALL
SELECT distinct action_type from {{ params.param_hr_stage_dataset_name }}.persacthst
UNION ALL
SELECT distinct action_type from {{ params.param_hr_stage_dataset_name }}.histerr) as src1
LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ref_action_type_stg as src2
on src1.action_type = src2.action_type_code

);

BEGIN TRANSACTION;
MERGE {{ params.param_hr_core_dataset_name }}.ref_action_type tgt
USING T1 src
ON src.action_type = tgt.action_type_code
WHEN MATCHED THEN
UPDATE SET dw_last_update_date_time = ts
WHEN not matched THEN
INSERT(action_type_code, action_type_desc, source_system_code, dw_last_update_date_time)
VALUES(action_type, action_type_desc, 'L', ts);

SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       action_type_code
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_action_type
    GROUP BY
       action_type_code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ref_action_type');
  ELSE
COMMIT TRANSACTION;
END IF;
END;

