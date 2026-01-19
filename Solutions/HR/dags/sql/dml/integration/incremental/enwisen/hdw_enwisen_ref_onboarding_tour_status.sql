BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_onboarding_tour_status AS tgt USING (
    SELECT
        CASE
          WHEN tgt_0.tour_status_id IS NOT NULL THEN tgt_0.tour_status_id
          ELSE (
            SELECT
                coalesce(max(ref_onboarding_tour_status.tour_status_id), CAST(0 as INTEGER))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_tour_status
          ) + CAST(row_number() OVER (ORDER BY tour_status_id, stg.tour_status_text) as INTEGER)
        END AS tour_status_id,
        stg.tour_status_text AS tour_status_text,
        stg.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              trim(status_state) AS tour_status_text,
              'B' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_hcm_resourcetransition_stg
            WHERE trim(status_state) IS NOT NULL
            GROUP BY 1, 2
        ) AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_tour_status AS tgt_0 ON tgt_0.tour_status_text = stg.tour_status_text
         AND upper(tgt_0.source_system_code) = upper(stg.source_system_code)
  ) AS src
  ON tgt.tour_status_id = src.tour_status_id
   AND upper(tgt.source_system_code) = upper(src.source_system_code)
     WHEN MATCHED THEN UPDATE SET dw_last_update_date_time = src.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (tour_status_id, tour_status_text, source_system_code, dw_last_update_date_time) VALUES (cast(src.tour_status_id as int64), src.tour_status_text, src.source_system_code, src.dw_last_update_date_time)
  ;

  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT tour_status_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_onboarding_tour_status
      GROUP BY tour_status_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: ' , '{{ params.param_hr_core_dataset_name }}' , '.ref_onboarding_tour_status ');
  ELSE 
  COMMIT TRANSACTION;
  END IF;
END;