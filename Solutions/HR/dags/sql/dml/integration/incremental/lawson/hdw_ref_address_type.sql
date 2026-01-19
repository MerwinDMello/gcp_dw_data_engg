  BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE ts DATETIME;
  
  SET ts = datetime_trunc(current_datetime('US/Central'), second);

  BEGIN TRANSACTION;

  MERGE {{ params.param_hr_core_dataset_name }}.ref_address_type tgt
  USING {{ params.param_hr_stage_dataset_name }}.ref_address_type_stg src
  ON src.addr_type_code = tgt.addr_type_code
  AND src.addr_type_desc = tgt.addr_type_desc
  AND src.source_system_code = tgt.source_system_code
  WHEN MATCHED THEN
    UPDATE SET tgt.addr_type_code = src.addr_type_code,
               tgt.addr_type_desc = src.addr_type_desc,
               tgt.source_system_code = src.source_system_code,
               tgt.dw_last_update_date_time = ts
  WHEN NOT MATCHED THEN
  INSERT(addr_type_code, addr_type_desc, source_system_code, dw_last_update_date_time)
  VALUES(addr_type_code, addr_type_desc, source_system_code, ts);
  
  SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
       addr_type_code
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_address_type
    GROUP BY
       addr_type_code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ref_address_type');
  ELSE
COMMIT TRANSACTION;
END IF;
END;
