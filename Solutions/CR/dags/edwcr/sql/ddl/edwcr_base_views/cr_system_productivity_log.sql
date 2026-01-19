CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_system_productivity_log
   OPTIONS(description='Contains system productivity log details')
  AS SELECT
      cr_system_productivity_log.system_productivity_log_id,
      cr_system_productivity_log.cr_patient_id,
      cr_system_productivity_log.tumor_id,
      cr_system_productivity_log.system_user_id_code,
      cr_system_productivity_log.system_change_status_date,
      cr_system_productivity_log.source_system_code,
      cr_system_productivity_log.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_system_productivity_log
  ;
