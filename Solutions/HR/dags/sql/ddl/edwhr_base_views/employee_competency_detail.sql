CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.employee_competency_detail
AS SELECT
    employee_competency_detail.employee_competency_result_sid,
    employee_competency_detail.valid_from_date,
    employee_competency_detail.employee_talent_profile_sid,
    employee_competency_detail.employee_sid,
    employee_competency_detail.performance_plan_id,
    employee_competency_detail.competency_group_id,
    employee_competency_detail.competency_id,
    employee_competency_detail.evaluation_workflow_status_id,
    employee_competency_detail.review_period_id,
    employee_competency_detail.review_year_num,
    employee_competency_detail.review_period_start_date,
    employee_competency_detail.review_period_end_date,
    employee_competency_detail.employee_rating_num,
    employee_competency_detail.employee_rating_id,
    employee_competency_detail.manager_rating_num,
    employee_competency_detail.manager_rating_id,
    employee_competency_detail.manager_employee_rating_gap_num,
    employee_competency_detail.employee_num,
    employee_competency_detail.lawson_company_num,
    employee_competency_detail.process_level_code,
    employee_competency_detail.valid_to_date,
    employee_competency_detail.source_system_key,
    employee_competency_detail.source_system_code,
    employee_competency_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_competency_detail;
