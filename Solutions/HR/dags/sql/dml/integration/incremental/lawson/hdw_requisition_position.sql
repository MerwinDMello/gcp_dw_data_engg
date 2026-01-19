BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.requisition_position_wrk;

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.requisition_position_wrk (
    position_sid,
    requisition_sid,
    requisition_num,
    position_code,
    valid_from_date,
    valid_to_date,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  CAST(lkp_pos_sid.sk AS int64) AS position_sid,
  CAST(xwlk.sk AS int64) AS requisition_sid,
  pajob.requisition AS requisition_num,
  pajob.position AS position_code,
  current_ts AS valid_from_date,
  datetime("9999-12-31 23:59:59") AS valid_to_date,
  pajob.company AS lawson_company_num,
  TRIM(pajob.process_level) AS process_level_code,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.pajobreq AS pajob
  INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON UPPER(
    SUBSTR(
      CONCAT(
        TRIM(CAST(pajob.company AS string)),
        '-',
        TRIM(CAST(pajob.requisition AS string))
      ),
      1,
      255
    )
  ) = UPPER(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'REQUISITION'
  INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS lkp_pos_sid ON UPPER(
    SUBSTR(
      CONCAT(
        TRIM(CAST(pajob.company AS string)),
        '-',
        TRIM(COALESCE(pajob.position, ''))
      ),
      1,
      255
    )
  ) = UPPER(lkp_pos_sid.sk_source_txt)
  AND UPPER(lkp_pos_sid.sk_type) = 'POSITION'
  LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.requisition_position AS tgt ON xwlk.sk = tgt.requisition_sid
  AND UPPER(xwlk.sk_type) = 'REQUISITION'
  AND DATE(tgt.valid_to_date) = DATE '9999-12-31';

BEGIN TRANSACTION;

UPDATE
  {{ params.param_hr_core_dataset_name }}.requisition_position AS tgt
SET
  valid_to_date = (current_ts - INTERVAL 1 second)
WHERE
  DATE(tgt.valid_to_date) = DATE '9999-12-31'
  AND TRIM(CAST(tgt.requisition_sid AS string)) NOT IN(
    SELECT
      TRIM(
        CAST(
          requisition_position_wrk.requisition_sid AS string
        )
      )
    FROM
      {{ params.param_hr_stage_dataset_name }}.requisition_position_wrk
  ) AND tgt.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.requisition_position AS tgt
SET
  valid_to_date = datetime(current_ts) - INTERVAL 1 second,
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.requisition_position_wrk AS stg
WHERE
  TRIM(CAST(tgt.requisition_sid AS string)) = TRIM(
    COALESCE(CAST(stg.requisition_sid AS string), '')
  )
  AND DATE(tgt.valid_to_date) = DATE '9999-12-31'
  AND (
    TRIM(CAST(tgt.position_sid AS string)) <> TRIM(COALESCE(CAST(stg.position_sid AS string), ''))
    OR TRIM(CAST(tgt.lawson_company_num AS string)) <> TRIM(CAST(stg.lawson_company_num AS string))
    OR TRIM(tgt.process_level_code) <> TRIM(stg.process_level_code)
  ) AND tgt.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.requisition_position (
    position_sid,
    requisition_sid,
    requisition_num,
    position_code,
    valid_from_date,
    valid_to_date,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  requisition_position_wrk.position_sid,
  requisition_position_wrk.requisition_sid,
  requisition_position_wrk.requisition_num,
  requisition_position_wrk.position_code,
  current_ts,
  datetime("9999-12-31 23:59:59"),
  requisition_position_wrk.lawson_company_num,
  requisition_position_wrk.process_level_code,
  requisition_position_wrk.source_system_code,
  requisition_position_wrk.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.requisition_position_wrk
WHERE
  (
    TRIM(
      COALESCE(
        CAST(requisition_position_wrk.position_sid AS string),
        ''
      )
    ),
    TRIM(
      COALESCE(
        CAST(
          requisition_position_wrk.requisition_sid AS string
        ),
        ''
      )
    ),
    TRIM(
      CAST(
        requisition_position_wrk.requisition_num AS string
      )
    ),
    TRIM(requisition_position_wrk.position_code)
  ) NOT IN(
    SELECT
      AS STRUCT TRIM(
        CAST(requisition_position.position_sid AS string)
      ),
      TRIM(
        CAST(requisition_position.requisition_sid AS string)
      ),
      TRIM(
        CAST(
          requisition_position.requisition_num AS string
        )
      ),
      TRIM(requisition_position.position_code)
    FROM
      {{ params.param_hr_core_dataset_name }}.requisition_position
    WHERE
      DATE(requisition_position.valid_to_date) = DATE '9999-12-31'
  );

SET
  DUP_COUNT = (
    SELECT
      COUNT(*)
    FROM
      (
        SELECT
          Requisition_SID,
          Valid_From_Date
        FROM
          {{ params.param_hr_core_dataset_name }}.requisition_position
        GROUP BY
          Requisition_SID,
          Valid_From_Date
        HAVING
          COUNT(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = CONCAT(
  'Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.requisition_position'
);

ELSE COMMIT TRANSACTION;

END IF;

END;