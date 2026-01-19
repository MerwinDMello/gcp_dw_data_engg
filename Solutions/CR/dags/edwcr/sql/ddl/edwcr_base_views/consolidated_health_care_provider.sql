CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.consolidated_health_care_provider AS SELECT
    consolidated_health_care_provider.consolidated_provider_dw_id,
    consolidated_health_care_provider.company_code,
    consolidated_health_care_provider.coid,
    consolidated_health_care_provider.hcp_mnemonic_cs,
    consolidated_health_care_provider.facility_physician_num,
    consolidated_health_care_provider.hcp_mnemonic,
    consolidated_health_care_provider.hcp_active_ind,
    consolidated_health_care_provider.hcp_full_name,
    consolidated_health_care_provider.hcp_title,
    consolidated_health_care_provider.hcp_upin,
    consolidated_health_care_provider.hcp_specialty_code,
    consolidated_health_care_provider.provider_taxonomy_code,
    consolidated_health_care_provider.provider_login_code,
    consolidated_health_care_provider.national_provider_id,
    consolidated_health_care_provider.hcp_type_mnemonic_cs,
    consolidated_health_care_provider.clinical_system_module_code,
    consolidated_health_care_provider.hcp_mis_user_mnemonic,
    consolidated_health_care_provider.admit_privilege_ind,
    consolidated_health_care_provider.electronic_signature_code,
    consolidated_health_care_provider.network_mnemonic_cs,
    consolidated_health_care_provider.facility_mnemonic_cs,
    consolidated_health_care_provider.source_system_key,
    consolidated_health_care_provider.source_system_code,
    consolidated_health_care_provider.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.consolidated_health_care_provider
;
