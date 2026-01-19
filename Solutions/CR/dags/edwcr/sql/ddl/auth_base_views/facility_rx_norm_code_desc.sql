CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.facility_rx_norm_code_desc
AS select * from {{ params.param_clinical_dataset_name }}.facility_rx_norm_code_desc;

