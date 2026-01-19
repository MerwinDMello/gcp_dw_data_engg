CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.fact_imaging_result AS
SELECT *
FROM {{ params.param_auth_base_views_dataset_name }}.imag_rslt_dtl
WHERE imag_rslt_dtl.vld_to_ts = '9999-12-31 00:00:00'
;
