CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.clinical_registration AS SELECT
    clinical_registration.patient_dw_id,
    clinical_registration.company_code,
    clinical_registration.coid,
    clinical_registration.pat_acct_num,
    clinical_registration.patient_market_urn,
    clinical_registration.patient_facility_urn,
    clinical_registration.patient_facility_urn_abs,
    clinical_registration.medical_record_num,
    clinical_registration.ethnicity_code,
    clinical_registration.registrar_user_mnemonic_cs,
    clinical_registration.cancelled_registration_flag,
    clinical_registration.mother_patient_facility_urn,
    clinical_registration.mother_patient_facility_urn_abs,
    clinical_registration.network_mnemonic_cs,
    clinical_registration.facility_mnemonic_cs,
    clinical_registration.source_system_code,
    clinical_registration.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.clinical_registration
;
