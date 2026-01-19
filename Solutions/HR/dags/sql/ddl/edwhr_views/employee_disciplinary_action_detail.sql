/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee_disciplinary_action_detail AS SELECT
      a.employee_sid,
      a.disciplinary_ind,
      a.disciplinary_action_num,
      a.disciplinary_seq_num,
      a.valid_from_date,
      a.valid_to_date,
      a.eff_from_date,
      a.status_flag,
      a.follow_up_category_name,
      a.follow_up_type_name,
      a.follow_up_outcome_desc,
      a.follow_up_performed_by_employee_sid,
      a.follow_up_comment_desc,
      a.last_update_date,
      CASE
        WHEN session_user() = hr.userid THEN a.last_update_user_34_login_code
        ELSE '***'
      END AS last_update_user_34_login_code,
      a.employee_num,
      a.lawson_company_num,
      a.process_level_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_disciplinary_action_detail AS a
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            {{params.param_sec_base_views_dataset_name}}.security_mask_and_exception
          WHERE security_mask_and_exception.userid = session_user()
           AND upper(security_mask_and_exception.masked_column_code) = 'HR'
      ) AS hr ON hr.userid = session_user()
  ;

