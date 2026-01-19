CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_major_payor_group AS SELECT
    ref_major_payor_group.major_payor_group_code,
    ref_major_payor_group.major_payor_group_desc,
    ref_major_payor_group.major_payor_group_convert_ind,
    ref_major_payor_group.sub_payor_group_code,
    ref_major_payor_group.active_ind,
    ref_major_payor_group.source_system_code,
    ref_major_payor_group.dw_last_update_date_time
  FROM
    {{ params.param_pf_base_views_dataset_name }}.ref_major_payor_group
;
