CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.bi_surgical_case_detail
AS select * from {{ params.param_clinical_dataset_name }}.bi_surgical_case_detail;

