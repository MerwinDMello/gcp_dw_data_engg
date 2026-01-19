BEGIN DECLARE current_ts datetime;

DECLARE DUP_COUNT INT64;

SET
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.requisition_status_wrk;

BEGIN TRANSACTION;

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.requisition_status_wrk (
    requisition_sid,
    status_sid,
    requisition_num,
    status_code,
    eff_from_date,
    eff_to_date,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  req.requisition_sid AS requisition_sid,
  sta.status_sid AS status_sid,
  pajob.requisition AS requisition_num,
  pajob.req_status AS status_code,
  current_date('US/Central') AS eff_from_date,
  date("9999-12-31") AS eff_to_date,
  pajob.company AS lawson_company_num,
  pajob.process_level AS process_level_code,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.pajobreq AS pajob
  INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS req ON trim(CAST(coalesce(pajob.company, 9999) as STRING)) = trim(
    CAST(coalesce(req.lawson_company_num, 9999) as STRING)
  )
  AND trim(
    CAST(coalesce(pajob.requisition, 9999) as STRING)
  ) = trim(
    CAST(coalesce(req.requisition_num, 9999) as STRING)
  )
  AND req.valid_to_date = datetime("9999-12-31 23:59:59")
  INNER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS sta ON trim(CAST(coalesce(pajob.company, 9999) as STRING)) = trim(
    CAST(coalesce(sta.lawson_company_num, 9999) as STRING)
  )
  AND upper(trim(coalesce(pajob.req_status, ''))) = upper(trim(coalesce(sta.status_code, '')))
  AND upper(sta.status_type_code) = 'REQ'
  AND CASE
    WHEN upper(trim(coalesce(pajob.process_level, ''))) = '' THEN '00000'
    ELSE trim(pajob.process_level)
  END = CASE
    WHEN upper(trim(coalesce(sta.process_level_code, ''))) = '' THEN '00000'
    ELSE trim(sta.process_level_code)
  END
  AND sta.valid_to_date = datetime("9999-12-31 23:59:59")
  LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_status AS tgt ON req.requisition_sid = tgt.requisition_sid
  AND tgt.valid_to_date = datetime("9999-12-31 23:59:59");

/* Test Unique Primary Index constarint set in Teradata*/
SET
  DUP_COUNT =(
    select
      count(*)
    from
      (
        select
          Requisition_SID,
          Status_SID,
          Eff_From_Date
        from
          {{ params.param_hr_stage_dataset_name }}.requisition_status_wrk
        group by
          Requisition_SID,
          Status_SID,
          Eff_From_Date
        having
          count(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat(
  'Duplicates are not allowed in the table: requisition_status_wrk'
);

ELSE COMMIT TRANSACTION;

END IF;

BEGIN TRANSACTION;

UPDATE
  {{ params.param_hr_core_dataset_name }}.requisition_status AS tgt
SET
  valid_to_date = current_ts - Interval 1 SECOND,
  dw_last_update_date_time = current_ts
WHERE
  tgt.valid_to_date = datetime("9999-12-31 23:59:59")
  AND (
    TRIM(CAST(tgt.requisition_sid AS STRING)),
    tgt.status_sid
  ) NOT IN(
    SELECT
      AS STRUCT trim(
        CAST(requisition_status_wrk.requisition_sid AS STRING)
      ),
      requisition_status_wrk.status_sid
    FROM
      {{ params.param_hr_stage_dataset_name }}.requisition_status_wrk
  )
  AND tgt.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.requisition_status AS tgt
SET
  valid_to_date = current_ts - Interval 1 SECOND,
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.requisition_status_wrk AS stg
WHERE
  trim(CAST(tgt.requisition_sid AS STRING)) = trim(CAST(stg.requisition_sid AS STRING))
  AND tgt.status_sid <> stg.status_sid
  AND (
    tgt.requisition_num <> stg.requisition_num
    OR tgt.status_code <> stg.status_code
    OR tgt.lawson_company_num <> stg.lawson_company_num
  )
  AND tgt.valid_to_date = datetime("9999-12-31 23:59:59")
  AND tgt.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.requisition_status (
    requisition_sid,
    status_sid,
    requisition_num,
    status_code,
    valid_from_date,
    valid_to_date,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  wrk.requisition_sid,
  wrk.status_sid,
  wrk.requisition_num,
  wrk.status_code,
  current_ts,
  datetime("9999-12-31 23:59:59"),
  wrk.lawson_company_num,
  wrk.process_level_code,
  wrk.source_system_code,
  wrk.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.requisition_status_wrk AS wrk
  LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.requisition_status AS tgt ON tgt.requisition_sid = wrk.requisition_sid
  AND tgt.valid_to_date = datetime("9999-12-31 23:59:59")
WHERE
  tgt.requisition_sid IS NULL;

/* Test Unique Primary Index constarint set in Teradata*/
SET
  DUP_COUNT =(
    select
      count(*)
    from
      (
        select
          Requisition_SID,
          Valid_From_Date
        from
          {{ params.param_hr_core_dataset_name }}.requisition_status
        group by
          Requisition_SID,
          Valid_From_Date
        having
          count(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat(
  'Duplicates are not allowed in the table:requisition_status'
);

ELSE COMMIT TRANSACTION;

END IF;

END;