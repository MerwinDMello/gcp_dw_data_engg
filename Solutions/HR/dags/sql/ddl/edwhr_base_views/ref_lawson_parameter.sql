create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_lawson_parameter`
AS SELECT
ref_lawson_parameter.parameter_code_1,
ref_lawson_parameter.parameter_code_2,
ref_lawson_parameter.parameter_code_3,
ref_lawson_parameter.parameter_group_code,
ref_lawson_parameter.detail_value_text,
ref_lawson_parameter.source_system_code,
ref_lawson_parameter.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_lawson_parameter;