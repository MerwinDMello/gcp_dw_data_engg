CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.ref_apr_risk_of_mortality AS
select * from {{ params.param_ops_project_id }}.auth_base_views.ref_apr_risk_of_mortality