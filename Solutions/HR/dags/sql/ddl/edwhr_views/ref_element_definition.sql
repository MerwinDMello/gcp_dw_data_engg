/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_element_definition AS SELECT
      a.element_detail_entity_text,
      a.element_detail_type_text,
      a.element_detail_definition_desc,
      a.element_detail_definition_type_id,
      a.element_definition_selection_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_element_definition AS a
  ;

