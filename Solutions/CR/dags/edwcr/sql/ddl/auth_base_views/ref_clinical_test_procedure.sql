CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.ref_clinical_test_procedure
AS select * from {{ params.param_clinical_dataset_name }}.ref_clinical_test_procedure;