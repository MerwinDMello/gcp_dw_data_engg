CREATE OR REPLACE VIEW `{{ params.param_auth_base_views_dataset_name }}.payor_organization`
AS select * from {{ params.param_ops_project_id }}.auth_base_views.payor_organization;