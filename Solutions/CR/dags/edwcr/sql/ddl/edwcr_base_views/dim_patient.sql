CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_patient AS 
SELECT * FROM {{ params.param_auth_base_views_dataset_name }}.ptnt_dtl
;
