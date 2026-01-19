BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_posting_board_type (posting_board_type_id, posting_board_type_code, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              coalesce(max(posting_board_type_id), CAST(0 as INT64))
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_posting_board_type
        ) + CAST(row_number() OVER (ORDER BY upper(stgg.posting_board_type_code)) as INT64) AS posting_board_type_id,
        stgg.posting_board_type_code AS posting_board_type_code,
        stgg.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              substr(concat(trim(taleo_jobinformation.postingboardtype), repeat(' ', 10)), 1, 10) AS posting_board_type_code,
              'T' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.taleo_jobinformation
          UNION DISTINCT
          SELECT DISTINCT
              substr(concat(trim(ats_hcm_jobboard_stg.boardtype_state), repeat(' ', 10)), 1, 10) AS posting_board_type_code,
              'B' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_hcm_jobboard_stg
        ) AS stgg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_posting_board_type AS tgt ON coalesce(trim(tgt.posting_board_type_code), 'XXX') = coalesce(trim(stgg.posting_board_type_code), 'XXX')
         AND upper(trim(tgt.source_system_code)) = upper(trim(stgg.source_system_code))
      WHERE tgt.posting_board_type_id IS NULL
  ;

  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT posting_board_type_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_posting_board_type
      GROUP BY posting_board_type_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: ref_posting_board_type ');
  ELSE 
  COMMIT TRANSACTION;
  END IF;
END;