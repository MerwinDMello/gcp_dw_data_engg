CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_race_cl AS SELECT
    ref_race.company_code,
    ref_race.coid,
    ref_race.race_mnemonic_cs,
    ref_race.active_ind,
    ref_race.race_name,
    ref_race.race_billing_code,
    ref_race.clinical_system_module_code,
    ref_race.network_mnemonic_cs,
    ref_race.facility_mnemonic_cs,
    ref_race.source_system_code,
    ref_race.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_race
;
