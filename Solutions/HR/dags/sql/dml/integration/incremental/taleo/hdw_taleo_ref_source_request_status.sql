BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;
  
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_source_request_status (source_request_status_id, source_request_status_desc, source_system_code, dw_last_update_date_time)
    SELECT
        stgg.source_request_status_id,
        stgg.source_request_status_desc,
        stgg.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              CASE
                 trim(taleo_src_request_status_stg.src_request_status_number)
                WHEN '' THEN 0
                ELSE CAST(trim(taleo_src_request_status_stg.src_request_status_number) as INT64)
              END AS source_request_status_id,
              trim(taleo_src_request_status_stg.description) AS source_request_status_desc,
              'T' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.taleo_src_request_status_stg
          UNION DISTINCT
          SELECT DISTINCT
              CAST(CASE
                 (ats_hcm_jobposting_stg.postingstatus)
                WHEN NULL THEN 0.0
                ELSE CAST((ats_hcm_jobposting_stg.postingstatus) as FLOAT64)
              END + 1000 as INT64) AS source_request_status_id,
              CASE
                WHEN CASE
                   (ats_hcm_jobposting_stg.postingstatus)
                  WHEN NULL THEN 0.0
                  ELSE CAST((ats_hcm_jobposting_stg.postingstatus) as FLOAT64)
                END = 1 THEN 'Not Posted'
                WHEN CASE
                   (ats_hcm_jobposting_stg.postingstatus)
                  WHEN NULL THEN 0.0
                  ELSE CAST((ats_hcm_jobposting_stg.postingstatus) as FLOAT64)
                END = 2 THEN 'Posted'
                WHEN CASE
                   (ats_hcm_jobposting_stg.postingstatus)
                  WHEN NULL THEN 0.0
                  ELSE CAST((ats_hcm_jobposting_stg.postingstatus) as FLOAT64)
                END = 3 THEN 'Exported'
                WHEN CASE
                   (ats_hcm_jobposting_stg.postingstatus)
                  WHEN NULL THEN 0.0
                  ELSE CAST((ats_hcm_jobposting_stg.postingstatus) as FLOAT64)
                END = 4 THEN 'Submitted'
                ELSE ''
              END AS source_request_status_desc,
              'B' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_hcm_jobposting_stg
        ) AS stgg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_source_request_status AS tgt ON coalesce(CAST(tgt.source_request_status_id AS STRING), CAST(99999 as STRING)) = coalesce(trim(CAST(stgg.source_request_status_id as STRING)), CAST(99999 as STRING))
      WHERE tgt.source_request_status_id IS NULL
  ;



    /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Source_Request_Status_Id 
        from {{ params.param_hr_core_dataset_name }}.ref_source_request_status
        group by Source_Request_Status_Id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_source_request_status');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;