CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_diagnosis AS SELECT
    dc.diag_cd AS diagnosis_code,
    dc.diag_desc AS diagnosis_desc,
    sys.nm AS code_type_name,
    dc.umls_cui,
    dc.umls_aui,
    dc.smntc_type AS semantic_type,
    dc.smntc_type_desc AS semantic_type_desc,
    dc.vld_fr_ts AS valid_from_date_time,
    dc.dw_insrt_ts AS dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.diag_cd AS dc
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cd_sys_version AS version ON dc.cd_sys_version_sk = version.cd_sys_version_sk
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cd_sys AS sys ON version.cd_sys_sk = sys.cd_sys_sk
  WHERE dc.vld_to_ts = '9999-12-31 00:00:00'
   AND version.vld_to_ts = '9999-12-31 00:00:00'
   AND sys.vld_to_ts = '9999-12-31 00:00:00'
;
