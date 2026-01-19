-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_clinical_phrm_rxm_cl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_clinical_phrm_rxm_cl AS SELECT
    ref_clinical_phrm_rxm.clinical_phrm_rxm_mnemonic_cs,
    ref_clinical_phrm_rxm.company_code,
    ref_clinical_phrm_rxm.coid,
    ref_clinical_phrm_rxm.network_mnemonic_cs,
    ref_clinical_phrm_rxm.facility_mnemonic_cs,
    ref_clinical_phrm_rxm.clinical_phrm_rxm_ndc_code,
    ref_clinical_phrm_rxm.clinical_phrm_rxm_name,
    ref_clinical_phrm_rxm.clinical_phrm_rxm_fsv_trade_name,
    ref_clinical_phrm_rxm.clinical_phrm_rxm_generic_code,
    ref_clinical_phrm_rxm.clinical_phrm_rxm_generic_name,
    ref_clinical_phrm_rxm.clinical_phrm_rxm_type_name,
    ref_clinical_phrm_rxm.clinical_phrm_rxm_strength_code,
    ref_clinical_phrm_rxm.clinical_phrm_rxm_form_name,
    ref_clinical_phrm_rxm.clinical_phrm_rxm_unit_name,
    ref_clinical_phrm_rxm.rxm_trade_name,
    ref_clinical_phrm_rxm.rxm_brand_name,
    ref_clinical_phrm_rxm.rxm_display_strength_code,
    ref_clinical_phrm_rxm.rxm_display_dispense_form_name,
    ref_clinical_phrm_rxm.rxm_fdb_alt_strength_code,
    ref_clinical_phrm_rxm.rxm_database_name,
    ref_clinical_phrm_rxm.rxm_fsv_id_text,
    ref_clinical_phrm_rxm.rxm_last_update_date,
    ref_clinical_phrm_rxm.rxm_last_update_user_code,
    ref_clinical_phrm_rxm.active_ind,
    ref_clinical_phrm_rxm.over_the_counter_ind,
    ref_clinical_phrm_rxm.medical_equipment_ind,
    ref_clinical_phrm_rxm.dea_control_schedule_code,
    ref_clinical_phrm_rxm.source_system_code,
    ref_clinical_phrm_rxm.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcl_base_views.ref_clinical_phrm_rxm
;
