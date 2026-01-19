CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.scri_patient_id_history
AS SELECT * FROM `hca-hin-prod-cur-clinical.auth_base_views.scri_patient_id_history`;
