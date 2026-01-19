CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.ur_metric AS
select * from `{{ params.param_cm_project_id }}.auth_base_views.ur_metric`