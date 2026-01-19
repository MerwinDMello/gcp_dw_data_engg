CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_state AS SELECT
    state.state_code,
    state.state_num,
    state.state_name,
    state.source_system_code
  FROM
    {{ params.param_pf_base_views_dataset_name }}.state
;
