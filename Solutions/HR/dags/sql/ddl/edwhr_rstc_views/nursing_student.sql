/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.nursing_student AS SELECT
    a.student_sid,
    a.valid_from_date,
    a.valid_to_date,
    a.student_num,
    CASE
      WHEN session_user() = so.userid THEN a.student_ssn
      ELSE '***'
    END AS student_ssn,
    a.student_first_name,
    a.student_last_name,
    a.student_middle_name,
    a.birth_date,
    a.gender_code,
    a.ethnic_origin_desc,
    a.addr_sid,
    a.pell_grant_eligibility_ind,
    a.first_gen_college_grad_ind,
    a.employee_num,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.nursing_student AS a
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
