CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_element_detail AS SELECT
    ref_element_detail.element_detail_id,
    ref_element_detail.element_definition_selection_id,
    ref_element_detail.active_sw,
    ref_element_detail.element_code,
    ref_element_detail.element_desc,
    ref_element_detail.element_seq_num,
    ref_element_detail.complete_sw,
    ref_element_detail.eff_from_date_time,
    ref_element_detail.eff_to_date_time,
    ref_element_detail.last_modified_date_time,
    ref_element_detail.source_system_code,
    ref_element_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_element_detail
;
