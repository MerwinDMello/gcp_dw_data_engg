CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.phrm_ordr_to_mdctn_admn
AS select * from {{ params.param_clinical_dataset_name }}.phrm_ordr_to_mdctn_admn;