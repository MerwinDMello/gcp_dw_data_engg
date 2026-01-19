/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_element_definition_selection AS SELECT
      a.element_definition_selection_id,
      a.element_definition_selection_code,
      a.element_definition_selection_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_element_definition_selection AS a
  ;

