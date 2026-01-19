CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_assignment AS SELECT
    ref_assignment.asgn_sid,
    ref_assignment.asgn_src_sys_key,
    ref_assignment.entity_sid,
    ref_assignment.instu_sid,
    ref_assignment.asgn_interface_code_desc,
    ref_assignment.asgn_desc,
    ref_assignment.asgn_short_desc,
    ref_assignment.asgn_active_ind,
    ref_assignment.display_order,
    ref_assignment.source_system_code,
    ref_assignment.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_assignment
;
