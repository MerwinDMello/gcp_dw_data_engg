CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_discharge_status AS SELECT
    dim_discharge_status.discharge_status_sk,
    dim_discharge_status.discharge_status_code,
    dim_discharge_status.discharge_status_desc,
    dim_discharge_status.source_system_code,
    dim_discharge_status.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_discharge_status
;
