/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.department AS SELECT
      a.dept_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.process_level_sid,
      a.dept_code,
      a.dept_name,
      a.lawson_company_num,
      a.process_level_code,
      a.source_system_code,
      a.active_dw_ind,
      a.security_key_text,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.department AS a
    WHERE (a.process_level_code, a.lawson_company_num) IN(
      SELECT AS STRUCT
          /*
          INNER JOIN EDWHR_BASE_VIEWS.HR_SECREF_PROCESS_LEVEL C
          ON A.Process_Level_Code = C.Process_Level_Code
          AND A.Lawson_Company_Num = C.Lawson_Company_Num
          AND C.User_Id = Current_User;*/
          process_level_code,
          lawson_company_num
        FROM
          {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level
        WHERE user_id = session_user()
    )
  ;

