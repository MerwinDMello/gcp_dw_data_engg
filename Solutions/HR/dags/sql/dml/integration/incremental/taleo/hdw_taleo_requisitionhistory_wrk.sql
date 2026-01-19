/*  Truncate Worktable Table     */
BEGIN
  
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_history_wrk;


/*  Load Work Table with working Data */


  INSERT INTO {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_history_wrk (file_date, recruitment_requisition_sid, creation_date_time, requisition_status_id, valid_from_date, valid_to_date, closed_date_time, requisition_creator_user_sid, recruitier_owner_user_sid, source_system_code, dw_last_update_date_time)
    SELECT
        file_date,
        rec_req.recruitment_requisition_sid,
        stg.creationdate AS creation_date_time,
        stg.requisitionstate_number AS requisition_status_id,
        file_date AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.enddate AS closed_date_time,
        rec_user_cr.recruitment_user_sid AS requisition_creator_user_sid,
        rec_user_ow.recruitment_user_sid AS recruitier_owner_user_sid,
        stg.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_requisitionhistory AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rec_req ON stg.requisition_number = rec_req.requisition_num
         AND rec_req.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(rec_req.source_system_code) = 'T'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS rec_user_cr ON stg.creator_userno = rec_user_cr.recruitment_user_num
         AND rec_user_cr.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(rec_user_cr.source_system_code) = 'T'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS rec_user_ow ON stg.recruiterowner_userno = rec_user_ow.recruitment_user_num
         AND rec_user_ow.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(rec_user_ow.source_system_code) = 'T'
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    UNION ALL
    SELECT
        stg.file_date AS file_date,
        stg.recruitment_requisition_sid,
        stg.creation_date_time,
        stg.requisition_status_id,
        stg.valid_from_date AS valid_from_date,
        stg.valid_to_date,
        DATETIME(stg.closed_date_time),
        stg.requisition_creator_user_sid,
        stg.recruitier_owner_user_sid,
        stg.source_system_code AS source_system_code,
        stg.dw_last_update_date_time
      FROM
        (
          SELECT
              current_date() AS file_date,
              jobrequisition,
              status,
              rec_req.recruitment_requisition_sid,
              -- ,COALESCE(CAST(SUBSTRING(TRIM(TO_CHAR(STG.CREATESTAMP)),1,19) AS TIMESTAMP(0)),CAST('1901-01-01 00:00:00' AS TIMESTAMP(0)))AS CREATION_DATE_TIME
              coalesce(CAST(substr(trim(CAST(stg_0.entry_stamp as STRING)), 1, 19) as DATETIME), DATETIME '1901-01-01 00:00:00') AS creation_date_time,
              coalesce(CAST(substr(trim(CAST(stg_0.updatestamp as STRING)), 1, 19) as TIMESTAMP), TIMESTAMP '1901-01-01 00:00:00') AS update_date_time,
              rrs.requisition_status_id AS requisition_status_id,
              current_date() AS valid_from_date,
              DATETIME("9999-12-31 23:59:59") AS valid_to_date,
              stg_0.datemovedtocurrentstatus AS closed_date_time,
              rec_user_cr.recruitment_user_sid AS requisition_creator_user_sid,
              rec_user_ow.recruitment_user_sid AS recruitier_owner_user_sid,
              'B' AS source_system_code,
              DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_auditlog_stg AS stg_0
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rec_req ON stg_0.jobrequisition = rec_req.requisition_num
               AND rec_req.valid_to_date = DATETIME("9999-12-31 23:59:59")
               AND upper(rec_req.source_system_code) = 'B'
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_requisition_status AS rrs ON trim(stg_0.status_state) = rrs.status_desc
               AND upper(rrs.source_system_code) = 'B'
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS rec_user_cr ON stg_0.creator = rec_user_cr.recruitment_user_num
               AND rec_user_cr.valid_to_date = DATETIME("9999-12-31 23:59:59")
               AND upper(rec_user_cr.source_system_code) = 'B'
              LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS rec_user_ow ON stg_0.recruiter = rec_user_ow.recruitment_user_num
               AND rec_user_ow.valid_to_date = DATETIME("9999-12-31 23:59:59")
               AND upper(rec_user_ow.source_system_code) = 'B'
        ) AS stg
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, stg.jobrequisition, stg.status, stg.update_date_time,stg.dw_last_update_date_time
      QUALIFY row_number() OVER (PARTITION BY stg.jobrequisition, stg.creation_date_time, stg.status ORDER BY unix_seconds(stg.update_date_time) DESC) = 1
  ; 

END;