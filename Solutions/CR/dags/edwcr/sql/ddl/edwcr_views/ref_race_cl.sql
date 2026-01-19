-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/ref_race_cl.sql
-- Translated from: Teradata
-- Translated to: BigQuery

-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.ref_race_cl AS SELECT
    ref_race_cl.company_code,
    ref_race_cl.coid,
    ref_race_cl.race_mnemonic_cs,
    ref_race_cl.active_ind,
    ref_race_cl.race_name,
    ref_race_cl.race_billing_code,
    ref_race_cl.clinical_system_module_code,
    ref_race_cl.network_mnemonic_cs,
    ref_race_cl.facility_mnemonic_cs,
    ref_race_cl.source_system_code,
    ref_race_cl.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.ref_race_cl
;
