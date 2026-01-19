CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.cr_secref_line_of_business
AS select * from {{ params.param_cr_base_views_dataset_name }}.secref_line_of_business;