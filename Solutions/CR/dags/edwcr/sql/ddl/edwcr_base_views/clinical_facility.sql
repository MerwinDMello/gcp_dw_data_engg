CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.clinical_facility AS SELECT
    clinical_facility.company_code,
    clinical_facility.coid,
    clinical_facility.facility_mnemonic_cs,
    clinical_facility.unit_num,
    clinical_facility.network_mnemonic_cs,
    clinical_facility.facility_mnemonic,
    clinical_facility.facility_active_ind,
    clinical_facility.facility_desc,
    clinical_facility.facility_prefix,
    clinical_facility.daylight_savings_time_ind,
    clinical_facility.hca_active_ind,
    clinical_facility.harm_event_exp_part_fac_ind,
    clinical_facility.harm_event_fac_active_ind,
    clinical_facility.trauma_current_level_num,
    clinical_facility.ntdb_num,
    clinical_facility.source_system_code,
    clinical_facility.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.clinical_facility
;
