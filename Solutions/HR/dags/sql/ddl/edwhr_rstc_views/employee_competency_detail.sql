/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.employee_competency_detail AS SELECT
    a.employee_competency_result_sid,
    a.valid_from_date,
    a.employee_talent_profile_sid,
    a.employee_sid,
    a.performance_plan_id,
    a.competency_group_id,
    a.competency_id,
    a.evaluation_workflow_status_id,
    a.review_period_id,
    a.review_year_num,
    a.review_period_start_date,
    a.review_period_end_date,
    a.employee_rating_num,
    a.employee_rating_id,
    a.manager_rating_num,
    a.manager_rating_id,
    a.manager_employee_rating_gap_num,
    a.employee_num,
    a.lawson_company_num,
    a.process_level_code,
    a.valid_to_date,
    a.source_system_key,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
     {{params.param_hr_base_views_dataset_name}}.employee_competency_detail AS a
;