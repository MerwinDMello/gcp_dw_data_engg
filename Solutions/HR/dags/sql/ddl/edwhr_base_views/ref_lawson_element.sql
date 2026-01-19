create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_lawson_element`
AS SELECT
  ref_lawson_element.lawson_element_num,
  ref_lawson_element.source_system_code,
  ref_lawson_element.lawson_topic_code,
  ref_lawson_element.lawson_element_desc,
  ref_lawson_element.lawson_element_type_flag,
  ref_lawson_element.dw_last_update_date_time
FROM
 {{ params.param_hr_core_dataset_name }}.ref_lawson_element;