
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_future_role_attribute_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_future_role_attribute_wrk 
    SELECT DISTINCT
        coalesce(a.role_id, 'U'),
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              trim(employee_info.future_role1_leadership_level) AS role_id
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee_info
          UNION ALL
          SELECT DISTINCT
              trim(employee_info_0.future_role1_org_size_scope) AS role_id
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee_info AS employee_info_0
          UNION ALL
          SELECT DISTINCT
              trim(employee_info_1.future_role1_r_timeframe) AS role_id
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee_info AS employee_info_1
          UNION ALL
          SELECT DISTINCT
              trim(employee_info_2.future_role1_r_function_area) AS role_id
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee_info AS employee_info_2
          UNION ALL
          SELECT DISTINCT
              trim(employee_info_3.future_role2_leadership_level) AS role_id
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee_info AS employee_info_3
          UNION ALL
          SELECT DISTINCT
              trim(employee_info_4.future_role2_org_size_scope) AS role_id
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee_info AS employee_info_4
          UNION ALL
          SELECT DISTINCT
              trim(employee_info_5.future_role2_r_timeframe) AS role_id
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee_info AS employee_info_5
          UNION ALL
          SELECT DISTINCT
              trim(employee_info_6.future_role2_r_function_area) AS role_id
            FROM
              {{ params.param_hr_stage_dataset_name }}.employee_info AS employee_info_6
        ) AS a
  ;
