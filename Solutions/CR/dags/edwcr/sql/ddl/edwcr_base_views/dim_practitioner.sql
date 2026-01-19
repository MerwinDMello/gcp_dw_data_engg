CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_practitioner AS
SELECT *
FROM {{ params.param_auth_base_views_dataset_name }}.prctnr_dtl
WHERE prctnr_dtl.vld_to_ts = '9999-12-31 00:00:00'
;
