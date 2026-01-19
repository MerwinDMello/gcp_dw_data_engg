/*==============================================================*/
/* Table: Respondent_Detail                                     */
/*==============================================================*/
/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.respondent_detail AS SELECT
    a.respondent_id,
    a.survey_receive_date,
    a.respondent_type_code,
    a.survey_sid,
    CASE
      WHEN session_user() = hr.userid THEN a.respondent_3_4_id
      ELSE '***'
    END AS respondent_3_4_id,
    CASE
      WHEN session_user() = hr.userid THEN a.employee_num
      ELSE '***'
    END AS employee_num,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.respondent_detail AS a
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          {{params.param_sec_base_views_dataset_name}}.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND upper(security_mask_and_exception.masked_column_code) = 'HR'
    ) AS hr ON hr.userid = session_user()
  WHERE upper(a.source_system_code) <> 'H'
;
