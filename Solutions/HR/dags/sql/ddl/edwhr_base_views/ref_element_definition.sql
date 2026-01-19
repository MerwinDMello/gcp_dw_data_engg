CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_element_definition
AS SELECT
    ref_element_definition.element_detail_entity_text,
    ref_element_definition.element_detail_type_text,
    ref_element_definition.element_detail_definition_desc,
    ref_element_definition.element_detail_definition_type_id,
    ref_element_definition.element_definition_selection_id,
    ref_element_definition.source_system_code,
    ref_element_definition.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.ref_element_definition;