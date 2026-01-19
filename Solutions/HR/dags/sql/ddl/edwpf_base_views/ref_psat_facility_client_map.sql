
/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_pf_base_views_dataset_name }}.ref_psat_facility_client_map AS SELECT
    ref_psat_facility_client_map.company_code,
    ref_psat_facility_client_map.coid,
    ref_psat_facility_client_map.client_id,
    ref_psat_facility_client_map.location_mnemonic_cs,
    ref_psat_facility_client_map.sub_unit_num,
    ref_psat_facility_client_map.service_code,
    ref_psat_facility_client_map.facility_name,
    ref_psat_facility_client_map.survey_form_text,
    ref_psat_facility_client_map.clinic_flag,
    ref_psat_facility_client_map.location_exclusion_flag,
    ref_psat_facility_client_map.source_system_code,
    ref_psat_facility_client_map.dw_last_update_date_time
  FROM
    {{ params.param_pf_core_dataset_name }}.ref_psat_facility_client_map
;
