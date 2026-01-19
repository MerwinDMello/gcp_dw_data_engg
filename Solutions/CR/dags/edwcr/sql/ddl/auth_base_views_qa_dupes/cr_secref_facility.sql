CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility
AS select * from {{ params.param_cr_base_views_dataset_name }}.secref_facility;