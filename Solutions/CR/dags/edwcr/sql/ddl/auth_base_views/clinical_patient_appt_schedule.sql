CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.clinical_patient_appt_schedule
AS select * from {{ params.param_clinical_dataset_name }}.clinical_patient_appt_schedule;

