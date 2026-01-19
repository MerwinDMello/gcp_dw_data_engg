CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_visit_type AS SELECT
    dim_visit_type.visit_type_sk,
    dim_visit_type.visit_type_code,
    dim_visit_type.visit_type_desc,
    dim_visit_type.source_system_code,
    dim_visit_type.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_visit_type
;
