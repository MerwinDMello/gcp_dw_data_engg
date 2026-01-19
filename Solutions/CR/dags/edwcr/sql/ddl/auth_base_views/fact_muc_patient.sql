CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.fact_muc_patient
AS select * from {{ params.param_clinical_dataset_name }}.fact_muc_patient;

