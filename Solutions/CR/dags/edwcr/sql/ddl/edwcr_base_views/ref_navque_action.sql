CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_navque_action
   OPTIONS(description='This table contains action and descriptions performed by Navigator')
  AS SELECT
      ref_navque_action.navque_action_id,
      ref_navque_action.navque_action_name,
      ref_navque_action.navque_action_desc,
      ref_navque_action.source_system_code,
      ref_navque_action.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_navque_action
  ;
