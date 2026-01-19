/*  DELETE FROM WORK TABLE */
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.offer_status_wrk;

/*  LOAD WORK TABLE WITH WORKING DATA */
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.offer_status_wrk (file_date, offer_sid, valid_from_date, offer_status_id, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        stg.file_date,
        ofr.offer_sid AS recruitment_requisition_sid,
        datetime_trunc(current_datetime('US/Central'), second) AS valid_from_date,
        cast(stg.status_number as INT64) AS offer_status_id,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        'T' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), second) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_offer AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS ofr ON CASE
           cast(stg.offer_number as string)
          WHEN '' THEN 0
          ELSE CAST(stg.offer_number as INT64)
        END = ofr.offer_num
         AND upper(ofr.source_system_code) = 'T'
         AND ofr.valid_to_date = DATETIME("9999-12-31 23:59:59")
      GROUP BY 1, 2, 3, 4, 5, 6, 7
  ;


  INSERT INTO {{ params.param_hr_stage_dataset_name }}.offer_status_wrk (file_date, offer_sid, valid_from_date, offer_status_id, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        CURRENT_DATE('US/Central') AS file_date,
        CAST(xwlk.sk AS INT64) AS offer_sid,
        datetime_trunc(current_datetime('US/Central'), second) AS valid_from_date,
        os.offer_status_id AS offer_status_id,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        'B' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), second) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON trim(concat(jobapplication, '-', candidate, '-', jobrequisition, '-ATS')) = trim(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'OFFER'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS os 
        ON COALESCE(UPPER(TRIM(stg.offerstatus_state) ),'')= COALESCE(UPPER(TRIM(os.offer_status_desc)),'')
         AND upper(os.source_system_code) = 'B'
      WHERE stg.offerstatus <> 0
      QUALIFY row_number() OVER (PARTITION BY stg.jobapplication, stg.candidate, stg.jobrequisition ORDER BY stg.updatestamp DESC) = 1
  ;
