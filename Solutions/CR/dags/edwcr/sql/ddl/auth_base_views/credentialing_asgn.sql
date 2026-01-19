CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.credentialing_asgn
AS select * from {{ params.param_psg_dataset_name }}.credentialing_asgn;




