CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_anesthesia AS SELECT
    dim_anesthesia.anesthesia_sk,
    dim_anesthesia.anesthesia_code,
    dim_anesthesia.anesthesia_desc,
    dim_anesthesia.source_system_code,
    dim_anesthesia.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_anesthesia
;
