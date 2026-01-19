BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.requisition_department_wrk;

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.requisition_department_wrk (
    dept_sid,
    requisition_sid,
    valid_from_date,
    valid_to_date,
    lawson_company_num,
    process_level_code,
    dept_code,
    requisition_num,
    security_key_text,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  dep.dept_sid AS dept_sid,
  req.requisition_sid AS requisition_sid,
  current_ts AS valid_from_date,
  datetime("9999-12-31 23:59:59") AS valid_to_date,
  pareq.company AS lawson_company_num,
  trim(pareq.process_level) AS process_level_code,
  trim(pareq.department) AS dept_code,
  pareq.requisition AS requisition_num,
  CONCAT(
    SUBSTR(
      '00000',
      1,
      5 - LENGTH(TRIM(CAST(pareq.company AS string)))
    ),
    TRIM(CAST(pareq.company AS string)),
    '-',
    CASE
      WHEN TRIM(pareq.process_level) IS NULL
      OR TRIM(pareq.process_level) = '' THEN '00000'
      ELSE CONCAT(
        SUBSTR(
          '00000',
          1,
          5 - LENGTH(TRIM(pareq.process_level))
        ),
        TRIM(pareq.process_level)
      )
    END,
    '-',
    CASE
      WHEN TRIM(pareq.department) IS NULL
      OR TRIM(pareq.department) = '' THEN '00000'
      ELSE CONCAT(
        SUBSTR('00000', 1, 5 - LENGTH(TRIM(pareq.department))),
        TRIM(pareq.department)
      )
    END
  ) AS security_key_text,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.pajobreq AS pareq
  INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS req ON TRIM(
    CAST(COALESCE(pareq.company, 123456789) AS STRING)
  ) = TRIM(
    CAST(
      COALESCE(req.lawson_company_num, 123456789) AS STRING
    )
  )
  AND TRIM(
    CAST(COALESCE(pareq.requisition, 123456789) AS STRING)
  ) = TRIM(
    CAST(
      COALESCE(req.requisition_num, 123456789) AS STRING
    )
  )
  AND DATE(req.valid_to_date) = '9999-12-31'
  INNER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dep ON TRIM(
    CAST(COALESCE(pareq.company, 123456789) AS STRING)
  ) = TRIM(
    CAST(
      COALESCE(dep.lawson_company_num, 123456789) AS STRING
    )
  )
  AND TRIM(
    CAST(
      COALESCE(pareq.process_level, "123456789") AS STRING
    )
  ) = TRIM(
    CAST(
      COALESCE(dep.process_level_code, "123456789") AS STRING
    )
  )
  AND TRIM(
    CAST(
      COALESCE(pareq.department, "123456789") AS STRING
    )
  ) = TRIM(
    CAST(COALESCE(dep.dept_code, "123456789") AS STRING)
  )
  AND DATE(dep.valid_to_date) = '9999-12-31';

BEGIN TRANSACTION;

UPDATE
  {{ params.param_hr_core_dataset_name }}.requisition_department AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 second
WHERE
  DATE(tgt.valid_to_date) = DATE '9999-12-31'
  AND TRIM(CAST(tgt.requisition_sid AS string)) NOT IN(
    SELECT
      TRIM(
        CAST(
          requisition_department_wrk.requisition_sid AS string
        )
      )
    FROM
      {{ params.param_hr_stage_dataset_name }}.requisition_department_wrk
  ) AND tgt.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.requisition_department AS tgt
SET
  valid_to_date = datetime(current_ts) - INTERVAL 1 second,
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.requisition_department_wrk AS stg
WHERE
  tgt.requisition_sid = stg.requisition_sid
  AND DATE(tgt.valid_to_date) = DATE '9999-12-31'
  AND (
    TRIM(COALESCE(CAST(stg.dept_sid AS string), '')) <> TRIM(COALESCE(CAST(tgt.dept_sid AS string), ''))
    OR TRIM(COALESCE(stg.dept_code, '')) <> TRIM(COALESCE(tgt.dept_code, ''))
    OR TRIM(
      COALESCE(CAST(stg.requisition_num AS string), '')
    ) <> TRIM(
      COALESCE(CAST(tgt.requisition_num AS string), '')
    )
    OR TRIM(
      COALESCE(CAST(stg.lawson_company_num AS string), '')
    ) <> TRIM(
      COALESCE(CAST(tgt.lawson_company_num AS string), '')
    )
    OR TRIM(COALESCE(stg.security_key_text, '')) <> TRIM(COALESCE(tgt.security_key_text, ''))
    OR TRIM(COALESCE(stg.process_level_code, '')) <> TRIM(COALESCE(tgt.process_level_code, ''))
    OR TRIM(COALESCE(stg.source_system_code, '')) <> TRIM(COALESCE(tgt.source_system_code, ''))
  ) AND tgt.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.requisition_department (
    dept_sid,
    requisition_sid,
    valid_from_date,
    valid_to_date,
    lawson_company_num,
    process_level_code,
    dept_code,
    requisition_num,
    security_key_text,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  stg.dept_sid,
  stg.requisition_sid,
  current_ts,
  datetime("9999-12-31 23:59:59"),
  stg.lawson_company_num,
  stg.process_level_code,
  stg.dept_code,
  stg.requisition_num,
  stg.security_key_text,
  stg.source_system_code,
  current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.requisition_department_wrk AS stg
  LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS tgt ON tgt.requisition_sid = stg.requisition_sid
  AND DATE(tgt.valid_to_date) = '9999-12-31'
WHERE
  tgt.requisition_sid IS NULL;

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
          {{ params.param_hr_core_dataset_name }}.requisition_department
        GROUP BY
          Requisition_SID,
          Valid_From_Date
        HAVING
          COUNT(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = CONCAT(
  'Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.requisition_department'
);

ELSE COMMIT TRANSACTION;

END IF;

END;