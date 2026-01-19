/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_lawson_element AS SELECT
      a.lawson_element_num,
      a.source_system_code,
      a.lawson_topic_code,
      a.lawson_element_desc,
      a.lawson_element_type_flag,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_lawson_element AS a
  ;

