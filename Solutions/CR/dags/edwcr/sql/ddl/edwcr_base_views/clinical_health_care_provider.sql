CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.clinical_health_care_provider AS SELECT
    clinical_health_care_provider.hcp_dw_id,
    clinical_health_care_provider.company_code,
    clinical_health_care_provider.coid,
    clinical_health_care_provider.hcp_mnemonic_cs,
    clinical_health_care_provider.facility_physician_num,
    clinical_health_care_provider.hcp_mnemonic,
    clinical_health_care_provider.hcp_active_ind,
    clinical_health_care_provider.hcp_full_name,
    clinical_health_care_provider.hcp_title,
    clinical_health_care_provider.hcp_upin,
    clinical_health_care_provider.hcp_specialty_code,
    clinical_health_care_provider.national_provider_id,
    clinical_health_care_provider.hcp_type_mnemonic_cs,
    clinical_health_care_provider.clinical_system_module_code,
    clinical_health_care_provider.network_mnemonic_cs,
    clinical_health_care_provider.facility_mnemonic_cs,
    clinical_health_care_provider.source_system_code,
    clinical_health_care_provider.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.clinical_health_care_provider
;
