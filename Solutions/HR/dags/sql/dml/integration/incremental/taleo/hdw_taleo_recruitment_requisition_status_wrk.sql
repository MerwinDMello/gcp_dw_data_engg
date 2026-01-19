/*  DELETE FROM WORK TABLE */
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_status_wrk;

/*  LOAD WORK TABLE WITH WORKING DATA */
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_status_wrk (file_date, recruitment_requisition_sid, valid_from_date, valid_to_date, requisition_status_id, source_system_code, dw_last_update_date_time)
    SELECT
        stg.file_date,
        rr.recruitment_requisition_sid,
        datetime(stg.file_date) AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        CAST(stg.state_number as INT64) AS requisition_status_id,
        'T' AS source_system_code,
        TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_requisition AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr ON CASE
           trim(stg.number)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.number) as INT64)
        END = rr.requisition_num
      GROUP BY 1, 2, 1, 4, 5, 6, 7
  ;




  INSERT INTO {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_status_wrk (file_date, recruitment_requisition_sid, valid_from_date, valid_to_date, requisition_status_id, source_system_code, dw_last_update_date_time)
    SELECT
        a.file_date,
        a.recruitment_requisition_sid,
        a.valid_from_date,
        a.valid_to_date,
        a.requisition_status_id,
        a.source_system_code,
        a.dw_last_update_date_time
      FROM
        (
          SELECT
              CURRENT_DATE('US/Central') AS file_date,
              rr.recruitment_requisition_sid,
              CURRENT_DATETIME('US/Central') AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") AS valid_to_date,
              rs.requisition_status_id,
              'B' AS source_system_code,
              TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time,
              stg.updatestamp
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS stg
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr ON rr.requisition_num = stg.jobrequisition
               AND rr.valid_to_date = DATETIME("9999-12-31 23:59:59")
               AND upper(rr.source_system_code) = 'B'
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_requisition_status AS rs ON rs.status_desc = stg.status_state
               AND upper(rs.source_system_code) = 'B'
            GROUP BY 1, 2, 1, 4, 5, 6, 7, 8
            QUALIFY row_number() OVER (PARTITION BY rr.recruitment_requisition_sid ORDER BY stg.updatestamp DESC) = 1
        ) AS a
  ;
