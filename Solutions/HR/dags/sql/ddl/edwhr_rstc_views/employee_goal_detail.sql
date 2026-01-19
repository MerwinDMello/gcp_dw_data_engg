/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.employee_goal_detail AS SELECT
    a.employee_goal_detail_sid,
    a.valid_from_date,
    a.employee_talent_profile_sid,
    a.employee_sid,
    a.employee_goal_year_num,
    a.goal_name,
    a.goal_category_id,
    a.goal_weight_pct,
    a.expected_result_text,
    a.goal_measurement_text,
    a.goal_status_id,
    a.goal_progress_status_id,
    a.goal_performance_plan_id,
    a.goal_due_date,
    a.user_defined_date,
    a.review_year_num,
    a.review_period_end_date,
    a.review_period_start_date,
    a.review_period_id,
    a.manager_goal_performance_rating_id,
    a.manager_goal_performance_rating_num,
    a.employee_goal_performance_rating_id,
    a.employee_goal_performance_rating_num,
    a.employee_num,
    a.lawson_company_num,
    a.process_level_code,
    a.valid_to_date,
    a.source_system_key,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.employee_goal_detail AS a
;
