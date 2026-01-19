/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee_performance_review AS SELECT
      a.employee_sid,
      a.review_sequence_num,
      a.valid_from_date,
      a.valid_to_date,
      a.reviewer_employee_sid,
      a.scheduled_review_date,
      a.review_type_code,
      a.actual_review_date,
      a.performance_rating_code,
      a.last_update_date,
      a.last_update_time,
      a.last_updated_3_4_login_code,
      a.total_score_num,
      a.review_desc,
      a.review_schedule_type_code,
      a.next_review_date,
      a.next_review_code,
      a.last_review_date,
      a.employee_num,
      a.lawson_company_num,
      a.process_level_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_performance_review AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

