CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_future_role_attribute
AS SELECT
    ref_future_role_attribute.future_role_attribute_id,
    ref_future_role_attribute.future_role_attribute_desc,
    ref_future_role_attribute.source_system_code,
    ref_future_role_attribute.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_future_role_attribute;