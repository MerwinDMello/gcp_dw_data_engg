  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_resource_wrk;
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_resource_wrk (resource_screening_package_num, valid_from_date, candidate_sid, recruitment_requisition_sid, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        stg.resourcescreeningpackage AS resource_screening_package_num,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
        can.candidate_sid,
        req.recruitment_requisition_sid,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        'B' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_resourcescreeningpackage_stg AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS can ON can.candidate_num = stg.candidate
         AND (can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(can.source_system_code) = 'B'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS req ON req.requisition_num = stg.jobrequisition
         AND (req.valid_to_date) = DATETIME("9999-12-31 23:59:59")
         AND upper(req.source_system_code) = 'B'
      WHERE upper(stg.screeningpackage) LIKE 'B%'
      GROUP BY 1, 2, 3, 4, 5, 6, 7
  ;
