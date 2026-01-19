



/*  Load Work Table with working Data */
BEGIN
  DECLARE DUP_COUNT INT64;
  BEGIN TRANSACTION;
  
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_profile_medium AS tgt USING (
    SELECT
        app_med.number AS profile_medium_id,
        trim(app_med.code) AS profile_medium_code,
        trim(app_med.description) AS profile_medium_desc,
        app_med.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_applicationmedium AS app_med
      GROUP BY 1, 2, 3, 4, 5
  ) AS stg
  ON tgt.profile_medium_id = stg.profile_medium_id
     WHEN MATCHED THEN UPDATE SET profile_medium_code = stg.profile_medium_code, profile_medium_desc = stg.profile_medium_desc, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (profile_medium_id, profile_medium_code, profile_medium_desc, source_system_code, dw_last_update_date_time) VALUES (stg.profile_medium_id, stg.profile_medium_code, stg.profile_medium_desc, stg.source_system_code, stg.dw_last_update_date_time)
  ;
  SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            profile_medium_id
        from {{ params.param_hr_core_dataset_name }}.ref_profile_medium
        group by profile_medium_id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_profile_medium');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;
