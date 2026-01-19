CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_system_user
   OPTIONS(description='Contains details of the system user')
  AS SELECT
      cr_system_user.security_id,
      cr_system_user.user_id_code,
      cr_system_user.user_first_name,
      cr_system_user.user_last_name,
      cr_system_user.source_system_code,
      cr_system_user.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_system_user
  ;
