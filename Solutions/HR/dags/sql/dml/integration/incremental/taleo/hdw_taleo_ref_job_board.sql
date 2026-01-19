BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_job_board (job_board_id, job_board_type_id, recruitment_source_id, source_system_code, dw_last_update_date_time)
    SELECT
        stgg.job_board_id,
        stgg.job_board_type_id,
        CAST(stgg.recruitment_source_id AS INT64),
        stgg.source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              CASE
                 trim(taleo_job_board_stg.job_board_number)
                WHEN '' THEN 0
                ELSE CAST(trim(taleo_job_board_stg.job_board_number) as INT64)
              END AS job_board_id,
              CASE
                 trim(taleo_job_board_stg.jobboardtype_number)
                WHEN '' THEN 0
                ELSE CAST(trim(taleo_job_board_stg.jobboardtype_number) as INT64)
              END AS job_board_type_id,
              trim(taleo_job_board_stg.recruitmentsource_number) AS recruitment_source_id,
              'T' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.taleo_job_board_stg
        ) AS stgg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_job_board AS tgt ON coalesce(tgt.job_board_id, 99999) = coalesce(stgg.job_board_id , 99999)
      WHERE tgt.job_board_id IS NULL
  ;
    SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT job_board_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_job_board
      GROUP BY job_board_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table:ref_job_board ');
  ELSE 
  COMMIT TRANSACTION;
  END IF;
END;