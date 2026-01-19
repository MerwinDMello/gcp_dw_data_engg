CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.ssecref AS SELECT
    span_code_override.lawson_company_num,
    span_code_override.employee_34_login_code,
    span_code_override.process_level_code
  FROM
    {{ params.param_hr_views_dataset_name }}.span_code_override
;
