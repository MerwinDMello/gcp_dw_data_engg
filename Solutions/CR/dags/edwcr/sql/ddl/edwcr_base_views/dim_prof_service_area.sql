CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_prof_service_area AS SELECT
    dim_prof_service_area.prof_service_area_sk,
    dim_prof_service_area.prof_service_area_code,
    dim_prof_service_area.prof_service_area_desc,
    dim_prof_service_area.source_system_code,
    dim_prof_service_area.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_prof_service_area
;
