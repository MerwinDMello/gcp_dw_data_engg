BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_posting_status (posting_status_id, posting_status_code, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              coalesce(max(posting_status_id), CAST(0 as INT64))
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_posting_status
        ) + CAST(row_number() OVER (ORDER BY upper(stgg.posting_status_code)) as INT64) AS posting_status_id,
        stgg.posting_status_code AS posting_status_code,
        stgg.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              substr(concat(trim(taleo_jobinformation.postingstatus), repeat(' ', 10)), 1, 10) AS posting_status_code,
              'T' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.taleo_jobinformation
          UNION DISTINCT
          SELECT DISTINCT
              CASE
                WHEN CASE
                   cast(ats_hcm_jobposting_stg.postingstatus as int64)
                  WHEN SAFE_CAST('' AS INT64) THEN 0
                  ELSE SAFE_CAST(ats_hcm_jobposting_stg.postingstatus as INT64)
                END = 1 THEN 'Not Posted'
                WHEN CASE
                   cast(ats_hcm_jobposting_stg.postingstatus as int64)
                  WHEN SAFE_CAST('' AS INT64) THEN 0
                  ELSE SAFE_CAST(ats_hcm_jobposting_stg.postingstatus as INT64)
                END = 2 THEN 'Posted    '
                WHEN CASE
                   cast(ats_hcm_jobposting_stg.postingstatus as int64)
                  WHEN SAFE_CAST('' AS INT64) THEN 0
                  ELSE SAFE_CAST(ats_hcm_jobposting_stg.postingstatus as INT64)
                END = 3 THEN 'Exported  '
                WHEN CASE
                   cast(ats_hcm_jobposting_stg.postingstatus as int64)
                  WHEN SAFE_CAST('' AS INT64) THEN 0
                  ELSE SAFE_CAST(ats_hcm_jobposting_stg.postingstatus as INT64)
                END = 4 THEN 'Submitted '
                ELSE '          '
              END AS posting_status_code,
              'B' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_hcm_jobposting_stg
        ) AS stgg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_posting_status AS tgt ON coalesce(trim(tgt.posting_status_code), 'XXX') = coalesce(trim(stgg.posting_status_code), 'XXX')
         AND upper(trim(tgt.source_system_code)) = upper(trim(stgg.source_system_code))
      WHERE tgt.posting_status_id IS NULL
  ;

  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT posting_status_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_posting_status
      GROUP BY posting_status_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: ref_posting_status ');
  ELSE 
  COMMIT TRANSACTION;
  END IF;
END;