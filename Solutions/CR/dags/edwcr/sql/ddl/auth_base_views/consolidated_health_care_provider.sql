CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.consolidated_health_care_provider
AS select * from {{ params.param_clinical_dataset_name }}.consolidated_health_care_provider;