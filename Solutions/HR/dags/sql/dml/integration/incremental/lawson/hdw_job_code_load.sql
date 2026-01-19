BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

set
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

call `{{ params.param_hr_core_dataset_name }}`.sk_gen(
  "{{ params.param_hr_stage_dataset_name }}",
  'jobcode',
  'trim(coalesce(cast(Company AS STRING),\'\'))  ||\'-\'|| trim(coalesce(Job_Code ,\'\'))||\'-\'||source_system_code',
  'Job_Code'
);

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.job_code_wrk;

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.job_code_wrk (
    job_code_sid,
    eff_from_date,
    valid_from_date,
    job_class_sid,
    hr_company_sid,
    lawson_company_num,
    job_code,
    job_code_desc,
    active_dw_ind,
    process_level_code,
    eeo_category_code,
    eeo_code,
    security_key_text,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  cast(a.job_code_sid as INT64),
  a.eff_from_date,
  current_ts,
  cast(a.job_class_sid as INT64),
  a.hr_company_sid,
  a.lawson_company_num,
  a.job_code,
  a.job_code_desc,
  a.active_dw_ind,
  a.process_level_code,
  a.eeo_category_code,
  a.eeo_code,
  a.security_key_text,
  datetime("9999-12-31 23:59:59"),
  a.source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  (
    SELECT
      xwlk.sk AS job_code_sid,
      jcd.effective_date AS eff_from_date,
      current_ts AS valid_from_date,
      coalesce(a_0.job_class_sid, NUMERIC '0') AS job_class_sid,
      hr.hr_company_sid AS hr_company_sid,
      jcd.company AS lawson_company_num,
      jcd.job_code AS job_code,
      trim(jcd.description) AS job_code_desc,
      'Y' AS active_dw_ind,
      '00000' AS process_level_code,
      jcd.eeo_cat AS eeo_category_code,
      jcd.eeo_sub_code AS eeo_code,
      concat(
        substr(
          '00000',
          1,
          5 - length(trim(cast(jcd.company as string)))
        ),
        trim(cast(jcd.company as string)),
        '-',
        '00000',
        '-',
        '00000'
      ) AS security_key_text,
      datetime("9999-12-31 23:59:59") AS valid_to_date,
      'L' AS source_system_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.jobcode AS jcd
      INNER JOIN
      /* Lookup to match surrogate key */
      {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(
        concat(
          substr(
            concat(
              trim(coalesce(cast(jcd.company as string), '')),
              '-',
              trim(coalesce(cast(jcd.job_code as string), ''))
            ),
            1,
            255
          ),
          '-',
          jcd.source_system_code
        )
      ) = upper(xwlk.sk_source_txt)
      AND upper(xwlk.sk_type) = 'JOB_CODE'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS a_0 ON jcd.company = a_0.lawson_company_num
      AND jcd.job_class = a_0.job_class_code
      AND (a_0.valid_to_date) = datetime("9999-12-31 23:59:59")
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_company AS hr ON jcd.company = hr.lawson_company_num
      AND (hr.valid_to_date) = datetime("9999-12-31 23:59:59")
    WHERE
      xwlk.sk IS NOT NULL
      AND hr.hr_company_sid IS NOT NULL
  ) as a;

BEGIN TRANSACTION;

UPDATE
  {{ params.param_hr_core_dataset_name }}.job_code AS jcode
SET
  valid_to_date = current_ts,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.job_code_wrk AS stg
WHERE
  trim(cast(jcode.job_code_sid as string)) = trim(cast(stg.job_code_sid as string))
  AND (
    trim(cast(jcode.job_class_sid as string)) <> trim(cast(stg.job_class_sid as string))
    OR upper(trim(coalesce(jcode.job_code_desc, '~'))) <> upper(trim(coalesce(stg.job_code_desc, '~')))
    OR coalesce(jcode.eff_from_date, DATE '1900-12-01') <> coalesce(stg.eff_from_date, DATE '1900-12-01')
    OR coalesce(jcode.eeo_code, -9) <> coalesce(stg.eeo_code, -9)
    OR upper(trim(coalesce(jcode.eeo_category_code, '~'))) <> upper(trim(coalesce(stg.eeo_category_code, '~')))
    OR jcode.source_system_code <> stg.source_system_code
  )
  AND (jcode.valid_to_date) = datetime("9999-12-31 23:59:59")
  AND jcode.source_system_code = 'L'
  AND jcode.lawson_company_num <> 300;

UPDATE
  {{ params.param_hr_core_dataset_name }}.job_code AS tgt
SET
  valid_to_date = current_ts,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
WHERE
  upper(tgt.active_dw_ind) = 'Y'
  AND (
    trim(tgt.job_code) || tgt.lawson_company_num || tgt.source_system_code
  ) NOT IN(
    SELECT
      trim(stg.job_code) || stg.company || stg.source_system_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.jobcode AS stg
  )
  AND tgt.source_system_code = 'L'
  and tgt.lawson_company_num <> 300;

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.job_code (
    job_code_sid,
    valid_from_date,
    eff_from_date,
    job_class_sid,
    hr_company_sid,
    lawson_company_num,
    job_code,
    job_code_desc,
    active_dw_ind,
    security_key_text,
    process_level_code,
    eeo_category_code,
    eeo_code,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  job_code_wrk.job_code_sid,
  current_ts,
  job_code_wrk.eff_from_date,
  job_code_wrk.job_class_sid,
  job_code_wrk.hr_company_sid,
  job_code_wrk.lawson_company_num,
  job_code_wrk.job_code,
  job_code_wrk.job_code_desc,
  job_code_wrk.active_dw_ind,
  job_code_wrk.security_key_text,
  job_code_wrk.process_level_code,
  job_code_wrk.eeo_category_code,
  job_code_wrk.eeo_code,
  datetime("9999-12-31 23:59:59"),
  job_code_wrk.source_system_code,
  current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.job_code_wrk
WHERE
  (
    trim(cast(job_code_wrk.job_code_sid as string)),
    trim(cast(job_code_wrk.job_class_sid as string)),
    upper(
      trim(
        coalesce(cast(job_code_wrk.job_code_desc as string), '')
      )
    ),
    coalesce(
      cast(job_code_wrk.eff_from_date as date),
      DATE '1900-01-01'
    )
  ) NOT IN(
    SELECT
      AS STRUCT trim(cast(job_code.job_code_sid as string)),
      trim(cast(job_code.job_class_sid as string)),
      upper(trim(coalesce(job_code.job_code_desc, ''))),
      coalesce(job_code.eff_from_date, DATE '1900-01-01')
    FROM
      {{ params.param_hr_core_dataset_name }}.job_code
    WHERE
      (job_code.valid_to_date) = datetime("9999-12-31 23:59:59")
  );

SET
  DUP_COUNT = (
    SELECT
      COUNT(*)
    FROM
      (
        SELECT
          Job_Code_SID,
          Valid_From_Date
        FROM
          {{ params.param_hr_core_dataset_name }}.job_code
        GROUP BY
          Job_Code_SID,
          Valid_From_Date
        HAVING
          COUNT(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = CONCAT(
  'Duplicates are not allowed in the table: job_code'
);

ELSE COMMIT TRANSACTION;

END IF;

/*Job_code table transaction is verified*/
END;