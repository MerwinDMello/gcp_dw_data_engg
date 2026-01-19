/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.candidate_background_check AS SELECT
    a.report_sid,
    a.valid_from_date,
    a.report_create_date_time,
    a.candidate_first_name,
    a.candidate_middle_name,
    a.candidate_last_name,
    CASE
      WHEN session_user() = so.userid THEN a.social_security_num
      ELSE '***'
    END AS social_security_num,
    a.report_completion_date_time,
    a.report_reopen_date_time,
    a.process_level_code,
    a.recruitment_requisition_num_text,
    a.days_elapsed_cnt,
    a.criminal_search_ordered_cnt,
    a.criminal_search_pending_cnt,
    a.motor_vehicle_record_ordered_cnt,
    a.motor_vehicle_record_pending_cnt,
    a.employment_verification_ordered_cnt,
    a.employment_verification_pending_cnt,
    a.education_verification_ordered_cnt,
    a.education_verification_pending_cnt,
    a.license_verification_ordered_cnt,
    a.license_verification_pending_cnt,
    a.personal_reference_ordered_cnt,
    a.personal_reference_pending_cnt,
    a.sanction_check_ordered_cnt,
    a.sanction_check_pending_cnt,
    a.report_num,
    a.valid_to_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_hr_base_views_dataset_name }}.candidate_background_check AS a
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          edw_sec_base_views.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND upper(security_mask_and_exception.masked_column_code) = 'SSN'
    ) AS so ON so.userid = session_user()
;
