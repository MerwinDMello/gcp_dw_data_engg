CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.hcp_facility_assignment
AS select * from {{ params.param_psg_dataset_name }}.hcp_facility_assignment;


