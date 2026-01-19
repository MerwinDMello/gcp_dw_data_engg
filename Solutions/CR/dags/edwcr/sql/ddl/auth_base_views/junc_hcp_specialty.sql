CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.junc_hcp_specialty
AS select * from {{ params.param_psg_dataset_name }}.junc_hcp_specialty;

