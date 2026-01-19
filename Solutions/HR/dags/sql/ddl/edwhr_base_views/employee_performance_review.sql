create or replace view `{{ params.param_hr_base_views_dataset_name }}.employee_performance_review`
AS SELECT
    employee_performance_review.employee_sid,
    employee_performance_review.review_sequence_num,
    employee_performance_review.valid_from_date,
    employee_performance_review.valid_to_date,
    employee_performance_review.reviewer_employee_sid,
    employee_performance_review.scheduled_review_date,
    employee_performance_review.review_type_code,
    employee_performance_review.actual_review_date,
    employee_performance_review.performance_rating_code,
    employee_performance_review.last_update_date,
    employee_performance_review.last_update_time,
    employee_performance_review.last_updated_3_4_login_code,
    employee_performance_review.total_score_num,
    employee_performance_review.review_desc,
    employee_performance_review.review_schedule_type_code,
    employee_performance_review.next_review_date,
    employee_performance_review.next_review_code,
    employee_performance_review.last_review_date,
    employee_performance_review.employee_num,
    employee_performance_review.lawson_company_num,
    employee_performance_review.process_level_code,
    employee_performance_review.source_system_code,
    employee_performance_review.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.employee_performance_review;