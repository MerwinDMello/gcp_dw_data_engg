CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.lab_tst_rslt_dtl
AS select * from {{ params.param_clinical_dataset_name }}.lab_tst_rslt_dtl;

