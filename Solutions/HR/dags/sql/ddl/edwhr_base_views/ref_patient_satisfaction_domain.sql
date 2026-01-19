CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_patient_satisfaction_domain AS SELECT
    ref_patient_satisfaction_domain.domain_id,
    ref_patient_satisfaction_domain.domain_desc,
    ref_patient_satisfaction_domain.domain_group_id,
    ref_patient_satisfaction_domain.domain_group_desc,
    ref_patient_satisfaction_domain.source_system_code,
    ref_patient_satisfaction_domain.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_patient_satisfaction_domain
;