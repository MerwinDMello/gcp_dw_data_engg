/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.candidate_person AS SELECT
      a.candidate_sid,
      a.valid_from_date,
      a.first_name,
      a.middle_name,
      a.last_name,
      a.maiden_name,
      CASE
        WHEN session_user() = so.userid THEN cast(a.social_security_num as string)
        ELSE '***'
      END AS social_security_num,
      a.email_address,
      a.birth_date,
      a.driver_license_num,
      a.driver_license_state_code,
      a.valid_to_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.candidate_person AS a
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            {{params.param_sec_base_views_dataset_name}}.security_mask_and_exception
          WHERE security_mask_and_exception.userid = session_user()
           AND upper(security_mask_and_exception.masked_column_code) = 'SSN'
      ) AS so ON so.userid = session_user()
  ;

