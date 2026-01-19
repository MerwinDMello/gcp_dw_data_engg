BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

set
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.hr_company_wrk;

call `{{ params.param_hr_core_dataset_name }}`.sk_gen(
  '{{ params.param_hr_stage_dataset_name }}',
  'glsystem',
  'trim(coalesce(cast(Company AS string),\'\'))',
  'HR_Company'
);

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.hr_company_wrk (
    hr_company_sid,
    valid_from_date,
    company_code,
    lawson_company_num,
    company_name,
    valid_to_date,
    active_dw_ind,
    security_key_text,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  CAST(xwlk.sk AS INT64) AS hr_company_sid,
  current_ts AS valid_from_date,
  'H' AS company_code,
  gls.company AS lawson_company_num,
  TRIM(gls.name) AS company_name,
  datetime("9999-12-31 23:59:59") AS valid_to_date,
  'Y' AS active_dw_ind,
  CONCAT(
    SUBSTR(
      '00000',
      1,
      5 - LENGTH(TRIM(CAST(GLS.COMPANY AS STRING)))
    ),
    TRIM(CAST(GLS.COMPANY AS STRING)),
    '-',
    '00000',
    '-',
    '00000'
  ) AS security_key_text,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.glsystem AS gls
  INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON UPPER(
    SUBSTR(
      TRIM(COALESCE(CAST(gls.company AS STRING), '')),
      1,
      255
    )
  ) = UPPER(xwlk.sk_source_txt)
  AND UPPER(xwlk.sk_type) = 'HR_COMPANY';

BEGIN TRANSACTION;

UPDATE
  {{ params.param_hr_core_dataset_name }}.hr_company AS hrc
SET
  valid_to_date = current_ts,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.hr_company_wrk AS stg
WHERE
  TRIM(CAST(hrc.hr_company_sid AS string)) = TRIM(CAST(stg.hr_company_sid AS string))
  AND UPPER(TRIM(hrc.company_code)) = UPPER(TRIM(stg.company_code))
  AND UPPER(TRIM(hrc.company_name)) <> UPPER(TRIM(stg.company_name))
  AND UPPER(TRIM(hrc.source_system_code)) = UPPER(TRIM(stg.source_system_code))
  AND UPPER(hrc.active_dw_ind) = 'Y'
  AND hrc.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.hr_company AS hrc
SET
  valid_to_date = current_ts,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
WHERE
  upper(hrc.active_dw_ind) = 'Y'
  AND hrc.source_system_code = 'L'
  AND hrc.lawson_company_num NOT IN(
    SELECT
      hr_company_wrk.lawson_company_num
    FROM
      {{ params.param_hr_stage_dataset_name }}.hr_company_wrk
  )
  AND hrc.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.hr_company (
    hr_company_sid,
    valid_from_date,
    company_code,
    lawson_company_num,
    company_name,
    valid_to_date,
    active_dw_ind,
    security_key_text,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  hr_company_wrk.hr_company_sid,
  hr_company_wrk.valid_from_date,
  hr_company_wrk.company_code,
  hr_company_wrk.lawson_company_num,
  hr_company_wrk.company_name,
  hr_company_wrk.valid_to_date,
  hr_company_wrk.active_dw_ind,
  hr_company_wrk.security_key_text,
  hr_company_wrk.source_system_code,
  hr_company_wrk.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.hr_company_wrk
WHERE
  (
    UPPER(
      TRIM(CAST(hr_company_wrk.hr_company_sid AS string))
    ),
    UPPER(
      TRIM(CAST(hr_company_wrk.company_code AS string))
    ),
    UPPER(TRIM(hr_company_wrk.company_name))
  ) NOT IN(
    SELECT
      AS STRUCT UPPER(TRIM(CAST(hr_company.hr_company_sid AS string))),
      UPPER(TRIM(hr_company.company_code)),
      UPPER(TRIM(hr_company.company_name))
    FROM
      {{ params.param_hr_core_dataset_name }}.hr_company
    WHERE
      UPPER(hr_company.active_dw_ind) = 'Y'
      AND hr_company.source_system_code = 'L'
  );

SET
  DUP_COUNT = (
    SELECT
      COUNT(*)
    FROM
      (
        SELECT
          HR_Company_SID,
          Valid_From_Date
        FROM
          {{ params.param_hr_core_dataset_name }}.hr_company
        GROUP BY
          HR_Company_SID,
          Valid_From_Date
        HAVING
          COUNT(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = CONCAT(
  'Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.hr_company'
);

ELSE COMMIT TRANSACTION;

END IF;

END