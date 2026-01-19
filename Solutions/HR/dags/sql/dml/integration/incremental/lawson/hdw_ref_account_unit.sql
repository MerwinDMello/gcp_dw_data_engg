  BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE ts DATETIME;
  
  SET ts = datetime_trunc(current_datetime('US/Central'), second);

  BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.ref_account_unit AS tgt SET dw_last_update_date_time = ts 
  FROM (select distinct(acct_unit) from {{ params.param_hr_stage_dataset_name }}.zxauxref) AS src
  WHERE tgt.account_unit_code = src.acct_unit
  AND tgt.source_system_code = 'L';

  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_account_unit
  SELECT DISTINCT
    acct_unit as account_unit_code,
    '' as account_unit_desc,
    'L' as source_system_code,
    ts as dw_last_update_date_time
  FROM {{ params.param_hr_stage_dataset_name }}.zxauxref
  WHERE acct_unit NOT IN (SELECT DISTINCT account_unit_code FROM {{ params.param_hr_core_dataset_name }}.ref_account_unit);

SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       account_unit_code
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_account_unit
    GROUP BY
       account_unit_code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ref_account_unit');
  ELSE
COMMIT TRANSACTION;
END IF;
END;
