CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.patient_satisfaction_domain_data_eoq AS SELECT
    patient_satisfaction_domain_data_eoq.time_period_name,
    patient_satisfaction_domain_data_eoq.company_code,
    patient_satisfaction_domain_data_eoq.corporate_code,
    patient_satisfaction_domain_data_eoq.group_code,
    patient_satisfaction_domain_data_eoq.division_code,
    patient_satisfaction_domain_data_eoq.market_code,
    patient_satisfaction_domain_data_eoq.parent_coid,
    patient_satisfaction_domain_data_eoq.coid,
    patient_satisfaction_domain_data_eoq.facility_claim_control_num,
    patient_satisfaction_domain_data_eoq.domain_id,
    patient_satisfaction_domain_data_eoq.organization_level_text,
    patient_satisfaction_domain_data_eoq.respondent_count,
    patient_satisfaction_domain_data_eoq.top_box_num,
    patient_satisfaction_domain_data_eoq.vendor_assigned_percentile_rank_num,
    patient_satisfaction_domain_data_eoq.reporting_period_text,
    patient_satisfaction_domain_data_eoq.source_system_code,
    patient_satisfaction_domain_data_eoq.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.patient_satisfaction_domain_data_eoq
;