/***************REJECT TABLE LOGIC********************/
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.fact_employee_availability_reject;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.fact_employee_availability_reject 
    SELECT
        stg.login,
        stg.employee_id,
        stg.last_name,
        stg.first_name,
        stg.middle_name,
        stg.job_family,
        stg.job_code,
        stg.position_title,
        stg.organization_and_hierarchy,
        stg.manager,
        stg.manager_employee_id,
        stg.willing_to_travel,
        stg.willing_travel_percentage,
        stg.employee_mobilty_preferences,
        stg.willing_to_relocate,
        stg.relocation_preferences,
        stg.calibrate_overall_perf_rating,
        stg.overall_performance_rating,
        stg.employee_promote_interest,
        stg.potential,
        stg.future_role1_leadership_level,
        stg.future_role1_r_function_area,
        stg.future_role1_org_size_scope,
        stg.future_role1_r_timeframe,
        stg.future_role2_leadership_level,
        stg.future_role2_r_function_area,
        stg.future_role2_org_size_scope,
        stg.future_role2_r_timeframe,
        stg.flight_risk,
        stg.flight_risk_timeframe,
        stg.external_flight_risk_driver,
        stg.secondary_flight_risk_driver,
        stg.jobs_pooled_for_count,
        stg.positions_talent_pooled_count,
        stg.positions_slated_for_count,
        stg.readiness_unknown,
        stg.readiness_others,
        stg.ready_now,
        stg.ready_6_11_months,
        stg.ready_12_18_months,
        stg.ready_18_24_months,
        stg.successors_count,
        stg.talent_pool_count,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
        CASE
          WHEN trim(stg.employee_id) IS NULL THEN 'Employee_ID is NULL'
          WHEN substr(trim(stg.employee_id), 1, 1) NOT IN(
            '1', '2', '3', '4', '5', '6', '7', '8', '9'
          ) THEN 'Employee_Id is alpha_numeric'
          ELSE CAST(0 as STRING)
        END AS reject_reason,
        'Fact_Employee_Availability' AS reject_stg_tbl_nm
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_info AS stg
      WHERE substr(trim(stg.employee_id), 1, 1) NOT IN('1', '2', '3', '4', '5', '6', '7', '8', '9')
       OR length(trim(stg.employee_id)) >= 13
       OR ( substr(trim(stg.employee_id), 1, 1) IN('1', '2', '3', '4', '5', '6', '7', '8', '9')
            AND CASE COALESCE(trim(stg.employee_id),'')
                 WHEN '' THEN 0
                 ELSE CAST(trim(stg.employee_id) as INT64)
                END NOT IN( SELECT employee_num AS employee_num
                            FROM {{ params.param_hr_base_views_dataset_name }}.employee))
  ;

/****************POPULATING WORK TABLE******************/
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.fact_employee_availability_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.fact_employee_availability_wrk (employee_talent_profile_sid, valid_from_date, employee_num, employee_sid, valid_to_date, jobs_pooled_for_cnt, employee_talent_pool_cnt, employee_successor_cnt, employee_ready_now_cnt, employee_ready_18_24_month_cnt, employee_ready_12_18_month_cnt, employee_ready_6_11_month_cnt, employee_other_readiness_cnt, employee_readiness_unknown_cnt, empl_slated_for_position_cnt, emp_talent_pooled_for_pos_cnt, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        COALESCE(safe_cast(etp.employee_talent_profile_sid AS INT64), 0)  AS employee_talent_profile_sid,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS valid_from_date,
        COALESCE(safe_cast(stg.employee_id AS INT64),0) AS employee_num,
        emp.employee_sid AS employee_sid,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        COALESCE(safe_cast(stg.jobs_pooled_for_count AS INT64), 0)  AS jobs_pooled_for_cnt,
        COALESCE(safe_cast(stg.talent_pool_count AS INT64), 0)  AS employee_talent_pool_cnt,
        COALESCE(safe_cast(stg.successors_count AS INT64), 0)  AS employee_successor_cnt,
        COALESCE(safe_cast(stg.ready_now AS INT64), 0)  AS employee_ready_now_cnt,
        COALESCE(safe_cast(stg.ready_18_24_months AS INT64), 0)  AS employee_ready_18_24_month_cnt,
        COALESCE(safe_cast(stg.ready_12_18_months AS INT64), 0)  AS employee_ready_12_18_month_cnt,
        COALESCE(safe_cast(stg.ready_6_11_months AS INT64), 0)  AS employee_ready_6_11_month_cnt,
        COALESCE(safe_cast(stg.readiness_others AS INT64), 0)  AS employee_other_readiness_cnt,
        COALESCE(safe_cast(stg.readiness_unknown AS INT64), 0)  AS employee_readiness_unknown_cnt,
        COALESCE(safe_cast(stg.positions_slated_for_count AS INT64), 0)  AS empl_slated_for_position_cnt,
        COALESCE(safe_cast(stg.positions_talent_pooled_count AS INT64), 0)  AS emp_talent_pooled_for_pos_cnt,
        COALESCE(safe_cast(etp.lawson_company_num AS INT64),0) AS lawson_company_num,
        trim(coalesce(CAST(etp.process_level_code AS STRING), '00000')) AS process_level_code,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_info AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_talent_profile AS etp 
        ON COALESCE(safe_cast(stg.employee_id AS INT64),0) = coalesce(etp.employee_num, 0)
           AND etp.valid_to_date = DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp 
        ON COALESCE(safe_cast(stg.employee_id AS INT64),0) = emp.employee_num
           AND emp.valid_to_date = DATETIME("9999-12-31 23:59:59")
           AND emp.lawson_company_num = COALESCE(safe_cast(substr(stg.job_code, 1, 4) AS INT64),0)
      WHERE substr(stg.employee_id, 1, 1) IN('1', '2', '3', '4', '5', '6', '7', '8', '9', '0')
       AND substr(stg.employee_id, 5, 1) IN('1', '2', '3', '4', '5', '6', '7', '8', '9', '0' )
       QUALIFY row_number() OVER (PARTITION BY etp.employee_talent_profile_sid ORDER BY etp.employee_talent_profile_sid) = 1
  ;
