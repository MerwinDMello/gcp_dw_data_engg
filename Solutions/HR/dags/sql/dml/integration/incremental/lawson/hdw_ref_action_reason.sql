/*Truncate records from Core table*/
BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts DATETIME;

SET
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

BEGIN TRANSACTION;

delete from
  {{ params.param_hr_core_dataset_name }}.ref_action_reason
where source_system_code='L';
  

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.ref_action_reason (
    action_reason_text,
    lawson_company_num,
    action_reason_desc,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  trim(paactreas.act_reason_cd) AS action_reason_text,
  paactreas.company AS lawson_company_num,
  trim(paactreas.description) AS action_reason_desc,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.paactreas
GROUP BY
  1,
  2,
  3,
  4;

SET
  DUP_COUNT = (
    SELECT
      COUNT(*)
    FROM
      (
        SELECT
          Action_Reason_Text
        FROM
          {{ params.param_hr_core_dataset_name }}.ref_action_reason
        GROUP BY
          Action_Reason_Text,
          Lawson_Company_Num
        HAVING
          COUNT(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = CONCAT(
  'Duplicates are not allowed in the table: ref_action_reason'
);

ELSE COMMIT TRANSACTION;

END IF;

END;