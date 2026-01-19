CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_navque_reason
   OPTIONS(description='This table contains reason and descriptions performed by Navigator')
  AS SELECT
      ref_navque_reason.navque_reason_id,
      ref_navque_reason.navque_reason_name,
      ref_navque_reason.navque_reason_desc,
      ref_navque_reason.source_system_code,
      ref_navque_reason.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_navque_reason
  ;
