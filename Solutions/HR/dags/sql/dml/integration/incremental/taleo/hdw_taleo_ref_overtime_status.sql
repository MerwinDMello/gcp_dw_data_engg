BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_overtime_status AS tgt USING (
    SELECT
        (
          SELECT
              coalesce(max(overtime_status_id), CAST(0 as BIGNUMERIC))
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_overtime_status
        ) + CAST(row_number() OVER (ORDER BY stg.overtime_status_desc) as BIGNUMERIC) AS overtime_status_id,
        stg.overtime_status_desc,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              trim(description) AS overtime_status_desc,
              'T' AS source_system_code,
              timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.taleo_overtimestatus
          UNION ALL
          SELECT DISTINCT
              CASE
                WHEN upper(trim(exemptfromovertime_state)) = 'YES' THEN 'EXEMPT'
                WHEN upper(trim(exemptfromovertime_state)) = 'NO' THEN 'NON-EXEMPT'
                ELSE trim(exemptfromovertime_state)
              END AS overtime_status_desc,
              'B' AS source_system_code,
              timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg
        ) AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_overtime_status AS tgt_0 
        ON UPPER(trim(tgt_0.overtime_status_desc)) = UPPER(TRIM(stg.overtime_status_desc))
      WHERE tgt_0.overtime_status_id IS NULL
      QUALIFY row_number() OVER (PARTITION BY stg.overtime_status_desc ORDER BY upper(stg.source_system_code) DESC) = 1
  ) AS src
  ON tgt.overtime_status_id = src.overtime_status_id
     WHEN NOT MATCHED BY TARGET THEN INSERT (overtime_status_id, overtime_status_desc, source_system_code, dw_last_update_date_time) VALUES (CAST(src.overtime_status_id AS INT64), src.overtime_status_desc, src.source_system_code, timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND))
  ;
  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT overtime_status_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_overtime_status
      GROUP BY overtime_status_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: ref_overtime_status ');
  ELSE 
  COMMIT TRANSACTION;
  END IF;
END;