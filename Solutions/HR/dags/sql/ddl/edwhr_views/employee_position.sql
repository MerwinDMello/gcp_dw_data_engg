/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee_position AS SELECT
      a.employee_sid,
      a.position_sid,
      a.position_level_sequence_num,
      a.eff_from_date,
      a.valid_from_date,
      a.valid_to_date,
      a.eff_to_date,
      a.fte_percent,
      a.working_location_code,
      a.schedule_work_code,
      a.job_code,
      a.employee_num,
      CASE
        WHEN session_user() = hr.userid THEN cast(a.pay_rate_amt as string)
        ELSE '***'
      END AS pay_rate_amt,
      a.last_update_date,
      a.dept_sid,
      a.account_unit_num,
      a.gl_company_num,
      a.lawson_company_num,
      a.process_level_code,
      a.active_dw_ind,
      a.delete_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_position AS a
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            {{params.param_sec_base_views_dataset_name}}.security_mask_and_exception
          WHERE security_mask_and_exception.userid = session_user()
           AND upper(security_mask_and_exception.masked_column_code) = 'HR'
      ) AS hr ON hr.userid = session_user()
    WHERE (a.process_level_code, a.lawson_company_num) IN(
      SELECT AS STRUCT
          process_level_code,
          lawson_company_num
        FROM
          {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level
        WHERE user_id = session_user()
    )
  ;

