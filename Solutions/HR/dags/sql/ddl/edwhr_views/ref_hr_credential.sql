-- ------------------------------------------------------------------------------
/***************************************************************************************
   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_hr_credential AS SELECT
      ref_hr_credential.credential_code,
      ref_hr_credential.credential_type_code,
      ref_hr_credential.credential_group_desc,
      ref_hr_credential.credential_desc,
      ref_hr_credential.credential_report_desc,
      ref_hr_credential.source_system_code,
      ref_hr_credential.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_hr_credential
  ;

