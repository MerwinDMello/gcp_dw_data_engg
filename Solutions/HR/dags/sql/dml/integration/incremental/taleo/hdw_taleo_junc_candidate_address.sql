BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt=DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);


  BEGIN TRANSACTION;

  /*  Insert the New Records/Chnages into the Target Table  */ /*  RETIRE RECORD ON 2ND RETIRE LOGIC */
UPDATE
  {{ params.param_hr_core_dataset_name }}.junc_candidate_address AS tgt
SET
  valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND),
  dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.junc_candidate_address_wrk AS stg
WHERE
  tgt.valid_to_date =DATETIME("9999-12-31 23:59:59")
  AND tgt.candidate_sid = stg.candidate_sid
  AND TRIM(tgt.addr_type_code) = TRIM(stg.addr_type_code)
  AND tgt.addr_sid <> stg.addr_sid
  AND tgt.source_system_code = stg.source_system_code;


INSERT INTO
  {{ params.param_hr_core_dataset_name }}.junc_candidate_address (candidate_sid,
    valid_from_date,
    addr_sid,
    addr_type_code,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.candidate_sid,
  current_dt as valid_from_date,
  stg.addr_sid,
  stg.addr_type_code,
  DATETIME("9999-12-31 23:59:59") as valid_to_date,
  stg.source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.junc_candidate_address_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.junc_candidate_address AS tgt
ON
  stg.candidate_sid = tgt.candidate_sid
  AND stg.addr_sid = tgt.addr_sid
  AND stg.addr_type_code = tgt.addr_type_code
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.candidate_sid IS NULL ;


SET DUP_COUNT = ( 
        select count(*)
        from (
        select candidate_sid ,valid_from_date,addr_sid ,addr_type_code
        from {{ params.param_hr_core_dataset_name }}.junc_candidate_address
        group by candidate_sid ,valid_from_date,addr_sid ,addr_type_code
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.junc_candidate_address');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;
