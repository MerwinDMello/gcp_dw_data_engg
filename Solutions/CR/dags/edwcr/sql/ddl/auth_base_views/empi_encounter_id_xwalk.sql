CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.empi_encounter_id_xwalk
AS select * from {{ params.param_clinical_dataset_name }}.empi_encounter_id_xwalk;