CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_diagnosis_related_group AS SELECT
    drg_cd.drg_cd AS drg_code,
    drg_cd.drg_desc AS drg_desc,
    drg_cd.drg_type AS drg_type,
    drg_cd.drg_wt AS drg_weight,
    drg_cd.drg_geometric_mean_los AS drg_geometric_mean_los,
    drg_cd.drg_arthm_mean_los AS drg_arithmetic_mean_los,
    drg_cd.mdc_cd AS major_diagnosis_category_code,
    drg_cd.vld_fr_ts AS valid_from_date_time,
    drg_cd.dw_insrt_ts AS dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.drg_cd
  WHERE drg_cd.vld_to_ts = '9999-12-31 00:00:00'
;
