/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.junc_employee_address AS SELECT
      a.employee_sid,
      a.addr_sid,
      a.valid_from_date,
      a.addr_type_code,
      a.valid_to_date,
      a.employee_num,
      a.lawson_company_num,
      a.process_level_code,
      a.delete_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.junc_employee_address AS a
    WHERE (a.process_level_code, a.lawson_company_num) IN(
      SELECT AS STRUCT
          /*
          INNER JOIN EDWHR_BASE_VIEWS.HR_SECREF_PROCESS_LEVEL C
          ON A.Process_Level_Code = C.Process_Level_Code
          AND A.Lawson_Company_Num = C.Lawson_Company_Num
          AND C.User_Id = Current_User;*/
          a.process_level_code,
          a.lawson_company_num
        FROM
          {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level
        WHERE user_id = session_user()
    )
  ;

