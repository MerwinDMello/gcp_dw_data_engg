CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_ethnicity AS 
SELECT * 
FROM {{ params.param_auth_base_views_dataset_name }}.ref_ethnicity
;
