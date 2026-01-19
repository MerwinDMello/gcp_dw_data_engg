/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_element_detail AS SELECT
      a.element_detail_id,
      a.element_definition_selection_id,
      a.active_sw,
      a.element_code,
      a.element_desc,
      a.element_seq_num,
      a.complete_sw,
      a.eff_from_date_time,
      a.eff_to_date_time,
      a.last_modified_date_time,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_element_detail AS a
  ;

