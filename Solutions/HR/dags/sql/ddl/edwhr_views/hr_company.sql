/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hr_company AS SELECT
      a.hr_company_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.company_code,
      a.lawson_company_num,
      a.company_name,
      a.active_dw_ind,
      a.security_key_text,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.hr_company AS a
  ;

