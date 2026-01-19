/*Truncate records from Core table*/
BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

set
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.requisition_address_wrk;

BEGIN TRANSACTION;

/*  Load Work Table with working Data for First load*/
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.requisition_address_wrk (
    requisition_sid,
    addr_sid,
    requisition_num,
    valid_from_date,
    valid_to_date,
    source_system_code,
    lawson_company_num,
    process_level_code,
    dw_last_update_date_time
  )
SELECT
  coalesce(req.requisition_sid, 0) AS requisition_sid,
  -- done
  addr.addr_sid,
  pajr.requisition,
  current_ts AS valid_from_date,
  datetime("9999-12-31 23:59:59") AS valid_to_date,
  'L' AS source_system_code,
  pajr.company AS lawson_company_num,
  CASE
    WHEN upper(trim(coalesce(pajr.process_level, ''))) = '' THEN '00000'
    ELSE pajr.process_level
  END AS process_level_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.pajobreq AS pajr
  INNER JOIN (
    SELECT
      requisition.requisition_sid,
      requisition.requisition_num,
      requisition.lawson_company_num
    FROM
      {{ params.param_hr_base_views_dataset_name }}.requisition
    WHERE
      (requisition.valid_to_date) = datetime("9999-12-31 23:59:59")
      AND source_system_code = 'L'
    GROUP BY
      1,
      2,
      3
  ) AS req ON trim(cast(pajr.requisition as string)) = trim(cast(req.requisition_num as string))
  AND trim(cast(pajr.company as string)) = trim(cast(req.lawson_company_num as string))
  LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.pcodesdtl AS pcode ON upper(trim(coalesce(pajr.locat_code, ''))) = upper(trim(coalesce(pcode.code, '')))
  AND upper(pcode.type) = 'LO'
  INNER JOIN {{ params.param_hr_core_dataset_name }}.address AS addr ON upper(trim(coalesce(addr.addr_line_1_text, ''))) = upper(trim(coalesce(pcode.addr1, '')))
  AND upper(trim(coalesce(addr.addr_line_2_text, ''))) = upper(trim(coalesce(pcode.addr2, '')))
  AND upper(trim(coalesce(addr.addr_line_3_text, ''))) = upper(trim(coalesce(pcode.addr3, '')))
  AND upper(trim(coalesce(addr.addr_line_4_text, ''))) = upper(trim(coalesce(pcode.addr4, '')))
  AND upper(trim(coalesce(addr.city_name, ''))) = upper(trim(coalesce(pcode.city, '')))
  AND upper(trim(coalesce(addr.state_code, ''))) = upper(trim(coalesce(pcode.state, '')))
  AND upper(trim(coalesce(addr.zip_code, ''))) = upper(trim(coalesce(pcode.zip, '')))
  AND upper(trim(coalesce(addr.county_name, ''))) = upper(trim(coalesce(pcode.county, '')))
  AND upper(addr.addr_type_code) = 'LOC'
  AND upper(pcode.type) = 'LO'
  AND upper(trim(coalesce(addr.location_code, ''))) = upper(trim(coalesce(pcode.code, '')));

UPDATE
  {{ params.param_hr_core_dataset_name }}.requisition_address AS jradd
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.requisition_address_wrk AS stg
WHERE
  trim(cast(jradd.requisition_sid as string)) = trim(cast(stg.requisition_sid as string))
  AND trim(cast(jradd.addr_sid as string)) <> trim(cast(stg.addr_sid as string))
  AND jradd.valid_to_date = datetime("9999-12-31 23:59:59")
  AND jradd.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.requisition_address AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = current_ts
WHERE
  (tgt.valid_to_date) = datetime('9999-12-31 23:59:59')
  AND tgt.requisition_sid NOT IN(
    SELECT
      requisition_address_wrk.requisition_sid
    FROM
      {{ params.param_hr_stage_dataset_name }}.requisition_address_wrk
    GROUP BY
      1
  ) AND tgt.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.requisition_address (
    requisition_sid,
    addr_sid,
    requisition_num,
    valid_from_date,
    valid_to_date,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  requisition_address_wrk.requisition_sid,
  requisition_address_wrk.addr_sid,
  requisition_address_wrk.requisition_num,
  requisition_address_wrk.valid_from_date,
  requisition_address_wrk.valid_to_date,
  requisition_address_wrk.lawson_company_num,
  requisition_address_wrk.process_level_code,
  requisition_address_wrk.source_system_code,
  requisition_address_wrk.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.requisition_address_wrk
WHERE
  (
    trim(
      cast(
        requisition_address_wrk.requisition_sid as string
      )
    ),
    trim(cast(requisition_address_wrk.addr_sid as string)),
    trim(
      cast(
        requisition_address_wrk.requisition_num as string
      )
    )
  ) NOT IN(
    SELECT
      AS STRUCT trim(
        cast(requisition_address.requisition_sid as string)
      ),
      trim(cast(requisition_address.addr_sid as string)),
      trim(
        cast(requisition_address.requisition_num as string)
      )
    FROM
      {{ params.param_hr_core_dataset_name }}.requisition_address
    WHERE
      requisition_address.valid_to_date = datetime("9999-12-31 23:59:59")
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
          {{ params.param_hr_core_dataset_name }}.requisition_address
        GROUP BY
          Requisition_SID,
          Valid_From_Date
        HAVING
          COUNT(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = CONCAT(
  'Duplicates are not allowed in the table: .{{ params.param_hr_core_dataset_name }}.requisition_address'
);

ELSE COMMIT TRANSACTION;

END IF;

END;