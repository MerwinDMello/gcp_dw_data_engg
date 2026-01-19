BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_motive AS tgt USING (
    SELECT
        cswmotive_number,
        available,
        name,
        mnemonic,
        source_system_code,
        dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_cswmotive
  ) AS src
  ON tgt.motive_id = src.cswmotive_number
     WHEN MATCHED THEN UPDATE SET active_sw = src.available, motive_name = src.name, motive_code = src.mnemonic, source_system_code = src.source_system_code, dw_last_update_date_time = timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND)
     WHEN NOT MATCHED BY TARGET THEN INSERT (motive_id, active_sw, motive_name, motive_code, source_system_code, dw_last_update_date_time) VALUES (src.cswmotive_number, src.available, src.name, src.mnemonic, src.source_system_code, timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND))
  ;

    SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT motive_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_motive
      GROUP BY motive_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table:ref_motive ');
  ELSE 
  COMMIT TRANSACTION;
  END IF;

BEGIN TRANSACTION;
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_motive AS tgt USING (
    SELECT DISTINCT
        CASE
          WHEN motive_id IS NOT NULL THEN motive_id
          ELSE (
            SELECT
                coalesce(max(motive_id), CAST(100000 as BIGNUMERIC))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_motive
              WHERE upper(ref_motive.source_system_code) = 'B'
          ) + CAST(row_number() OVER (ORDER BY stg.motive_code) as BIGNUMERIC)
        END AS motive_id,
        stg.active_sw,
        stg.motive_name,
        stg.motive_code,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              active AS active_sw,
              trim(description) AS motive_name,
              trim(candidatedispositionreason) AS motive_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_hcm_candidatedispositionreason_stg
            WHERE CAST(active as INT64) = 1
             AND trim(candidatedispositionreason) <> ''
        ) AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_motive AS tgt_0 ON trim(tgt_0.motive_code) = trim(stg.motive_code)
         AND upper(tgt_0.source_system_code) = 'B'
  ) AS src
  ON tgt.motive_id = src.motive_id
     WHEN MATCHED THEN UPDATE SET active_sw = CAST(src.active_sw as INT64), motive_name = src.motive_name, motive_code = src.motive_code, source_system_code = src.source_system_code, dw_last_update_date_time = src.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (motive_id, active_sw, motive_name, motive_code, source_system_code, dw_last_update_date_time) VALUES (cast(src.motive_id as int64), CAST(src.active_sw as INT64), src.motive_name, src.motive_code, src.source_system_code, src.dw_last_update_date_time)
  ;
  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT motive_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_motive
      GROUP BY motive_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table:ref_motive ');
  ELSE 
  COMMIT TRANSACTION;
  END IF;
END;