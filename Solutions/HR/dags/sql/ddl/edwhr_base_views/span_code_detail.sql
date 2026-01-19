create or replace view `{{ params.param_hr_base_views_dataset_name }}.span_code_detail`
AS SELECT
span_code_detail.span_code,
span_code_detail.system_code,
span_code_detail.lawson_company_num,
span_code_detail.process_level_code,
span_code_detail.unit_num,
span_code_detail.coid,
span_code_detail.company_code,
span_code_detail.last_update_date,
span_code_detail.source_system_code,
span_code_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.span_code_detail;