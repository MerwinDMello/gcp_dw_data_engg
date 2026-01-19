CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_parm_facility_dtl AS SELECT
    ref_parm_facility_dtl.company_code,
    ref_parm_facility_dtl.coid,
    ref_parm_facility_dtl.parameter_eff_from_date,
    ref_parm_facility_dtl.parameter_mnemonic_cs,
    ref_parm_facility_dtl.parameter_code,
    ref_parm_facility_dtl.parameter_type_code,
    ref_parm_facility_dtl.parameter_eff_to_date,
    ref_parm_facility_dtl.network_mnemonic_cs,
    ref_parm_facility_dtl.facility_mnemonic_cs,
    ref_parm_facility_dtl.source_system_code,
    ref_parm_facility_dtl.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_parm_facility_dtl
;
