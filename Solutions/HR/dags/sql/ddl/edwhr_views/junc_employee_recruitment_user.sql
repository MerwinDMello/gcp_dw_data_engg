/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.junc_employee_recruitment_user AS SELECT
      a.employee_sid,
      a.recruitment_user_sid,
      a.valid_from_date,
      a.primary_facility_ind,
      a.valid_to_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.junc_employee_recruitment_user AS a
  ;

