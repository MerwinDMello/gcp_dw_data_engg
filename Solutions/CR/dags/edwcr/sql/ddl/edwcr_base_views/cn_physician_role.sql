CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_physician_role
   OPTIONS(description='This table contains all the roles a physician could play.')
  AS SELECT
      cn_physician_role.physician_id,
      cn_physician_role.physician_role_code,
      cn_physician_role.source_system_code,
      cn_physician_role.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_physician_role
  ;
