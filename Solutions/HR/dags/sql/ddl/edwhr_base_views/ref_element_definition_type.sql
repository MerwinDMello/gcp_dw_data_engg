CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_element_definition_type
AS SELECT
    ref_element_definition_type.element_detail_definition_type_id,
    ref_element_definition_type.element_detail_definition_type_desc,
    ref_element_definition_type.source_system_code,
    ref_element_definition_type.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.ref_element_definition_type;