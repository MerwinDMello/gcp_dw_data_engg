CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.hcm_avoid_denied_attribution AS
select * from `{{ params.param_cm_project_id }}.auth_base_views.hcm_avoid_denied_attribution`