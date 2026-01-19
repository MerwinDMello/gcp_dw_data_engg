CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_element_definition_selection
AS SELECT
    ref_element_definition_selection.element_definition_selection_id,
    ref_element_definition_selection.element_definition_selection_code,
    ref_element_definition_selection.element_definition_selection_name,
    ref_element_definition_selection.source_system_code,
    ref_element_definition_selection.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.ref_element_definition_selection;