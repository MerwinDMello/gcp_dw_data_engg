BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

set
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.process_level_address_wrk;

BEGIN TRANSACTION;

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.process_level_address_wrk (
    process_level_sid,
    addr_sid,
    valid_from_date,
    valid_to_date,
    lawson_company_num,
    process_level_code,
    security_key_text,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  -- XWLK.sk as Process_Level_SID,
  proc.process_level_sid AS process_level_sid,
  addr.addr_sid,
  current_ts AS valid_from_date,
  datetime("9999-12-31 23:59:59") AS valid_to_date,
  company AS lawson_company_num,
  CASE
    WHEN coalesce(trim(prs.process_level), '') = '' THEN '00000'
    ELSE prs.process_level
  END AS process_level_code,
  concat(
    substr(
      '00000',
      1,
      5 - length(trim(cast(prs.company as string)))
    ),
    trim(cast(prs.company as string)),
    '-',
    CASE
      WHEN trim(prs.process_level) IS NULL
      OR trim(prs.process_level) = '' THEN '00000'
      ELSE concat(
        substr('00000', 1, 5 - length(trim(prs.process_level))),
        trim(prs.process_level)
      )
    END,
    '-',
    '00000'
  ) AS security_key_text,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.prsystem AS prs
  INNER JOIN -- 3491
  {{ params.param_hr_base_views_dataset_name }}.process_level AS proc ON CASE
    WHEN coalesce(trim(prs.process_level), '') = '' THEN CAST(NULL as STRING)
    ELSE concat(
      substr('00000', 1, 5 - length(trim(prs.process_level))),
      trim(prs.process_level)
    )
  END = proc.process_level_code
  AND prs.company = proc.lawson_company_num
  AND upper(proc.source_system_code) = 'L'
  AND proc.valid_to_date = datetime("9999-12-31 23:59:59")
  LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.address AS addr ON upper(trim(coalesce(addr.addr_line_1_text, ''))) = upper(trim(coalesce(prs.addr1, '')))
  AND upper(trim(coalesce(addr.addr_line_2_text, ''))) = upper(trim(coalesce(prs.addr2, '')))
  AND upper(trim(coalesce(addr.addr_line_3_text, ''))) = upper(trim(coalesce(prs.addr3, '')))
  AND upper(trim(coalesce(addr.addr_line_4_text, ''))) = upper(trim(coalesce(prs.addr4, '')))
  AND upper(trim(coalesce(addr.city_name, ''))) = upper(trim(coalesce(prs.city, '')))
  AND upper(trim(coalesce(addr.state_code, ''))) = upper(trim(coalesce(prs.state, '')))
  AND upper(trim(coalesce(addr.zip_code, ''))) = upper(trim(coalesce(prs.zip, '')))
  AND upper(trim(coalesce(addr.country_code, ''))) = upper(trim(coalesce(prs.country_code, '')))
  AND upper(trim(coalesce(addr.addr_type_code, ''))) = 'PRS';

UPDATE
  {{ params.param_hr_core_dataset_name }}.process_level_address AS tgt
SET
  valid_to_date = current_datetime('US/Central'),
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.process_level_address_wrk AS stg
WHERE
  trim(cast(tgt.process_level_sid as string)) = trim(cast(stg.process_level_sid as string))
  AND trim(tgt.source_system_code) = trim(stg.source_system_code)
  AND coalesce(
    trim(cast(tgt.addr_sid as string)),
    CAST(1111 as STRING)
  ) <> coalesce(
    trim(cast(stg.addr_sid as string)),
    CAST(1111 as STRING)
  )
  AND (tgt.valid_to_date) = datetime("9999-12-31 23:59:59")
  AND tgt.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.process_level_address AS pla
SET
  valid_to_date = current_datetime('US/Central'),
  dw_last_update_date_time = current_ts
WHERE
  (pla.valid_to_date) = datetime("9999-12-31 23:59:59")
  AND (pla.process_level_sid, pla.source_system_code) NOT IN(
    SELECT
      AS STRUCT process_level_address_wrk.process_level_sid,
      process_level_address_wrk.source_system_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.process_level_address_wrk
    GROUP BY
      1,
      2
  )
  AND pla.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.process_level_address (
    process_level_sid,
    valid_from_date,
    addr_sid,
    valid_to_date,
    lawson_company_num,
    process_level_code,
    security_key_text,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  process_level_address_wrk.process_level_sid,
  process_level_address_wrk.valid_from_date,
  process_level_address_wrk.addr_sid,
  process_level_address_wrk.valid_to_date,
  process_level_address_wrk.lawson_company_num,
  process_level_address_wrk.process_level_code,
  process_level_address_wrk.security_key_text,
  process_level_address_wrk.source_system_code,
  process_level_address_wrk.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.process_level_address_wrk
WHERE
  (
    trim(
      cast(
        process_level_address_wrk.process_level_sid as string
      )
    ),
    trim(
      cast(process_level_address_wrk.addr_sid as string)
    ),
    trim(
      cast(
        process_level_address_wrk.source_system_code as string
      )
    )
  ) NOT IN(
    SELECT
      AS STRUCT (
        cast(
          process_level_address.process_level_sid as string
        )
      ),
      (
        cast(process_level_address.addr_sid as string)
      ),
      trim(process_level_address.source_system_code)
    FROM
      {{ params.param_hr_base_views_dataset_name }}.process_level_address
    WHERE
      (process_level_address_wrk.valid_to_date) = datetime("9999-12-31 23:59:59")
  );

SET
  DUP_COUNT = (
    SELECT
      COUNT(*)
    FROM
      (
        SELECT
          Process_Level_SID,
          Valid_From_Date
        FROM
          {{ params.param_hr_core_dataset_name }}.process_level_address
        GROUP BY
          Process_Level_SID,
          Valid_From_Date
        HAVING
          COUNT(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = CONCAT(
  'Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.Process_Level_Address'
);

ELSE COMMIT TRANSACTION;

END IF;

END