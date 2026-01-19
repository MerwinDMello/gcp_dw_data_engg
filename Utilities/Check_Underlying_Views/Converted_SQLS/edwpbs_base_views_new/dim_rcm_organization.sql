-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_rcm_organization.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_rcm_organization AS SELECT
    dim_rcm_organization.company_code,
    dim_rcm_organization.coid,
    dim_rcm_organization.customer_code,
    dim_rcm_organization.customer_short_name,
    dim_rcm_organization.customer_name,
    dim_rcm_organization.ssc_code,
    dim_rcm_organization.unit_num,
    dim_rcm_organization.facility_mnemonic,
    dim_rcm_organization.group_code,
    dim_rcm_organization.division_code,
    dim_rcm_organization.market_code,
    dim_rcm_organization.f_level,
    dim_rcm_organization.partnership_ind,
    dim_rcm_organization.go_live_date,
    dim_rcm_organization.eff_from_date,
    dim_rcm_organization.eff_to_date,
    dim_rcm_organization.ssc_alias_code,
    dim_rcm_organization.division_alias_code,
    dim_rcm_organization.ssc_name,
    dim_rcm_organization.ssc_alias_name,
    dim_rcm_organization.corporate_name,
    dim_rcm_organization.group_name,
    dim_rcm_organization.market_name,
    dim_rcm_organization.division_name,
    dim_rcm_organization.group_alias_name,
    dim_rcm_organization.market_alias_name,
    dim_rcm_organization.division_alias_name,
    dim_rcm_organization.ssc_coid,
    dim_rcm_organization.coid_name,
    dim_rcm_organization.facility_name,
    dim_rcm_organization.facility_state_code,
    dim_rcm_organization.medicare_expansion_ind,
    dim_rcm_organization.medicaid_conversion_vendor_name,
    dim_rcm_organization.facility_close_date,
    dim_rcm_organization.his_vendor_name,
    dim_rcm_organization.rcps_migration_date,
    dim_rcm_organization.discrepancy_threshold_amt,
    dim_rcm_organization.sma_high_dollar_threshold_amt,
    dim_rcm_organization.hsc_member_ind,
    dim_rcm_organization.clear_contract_ind,
    dim_rcm_organization.client_outbound_ind,
    dim_rcm_organization.him_conversion_date,
    dim_rcm_organization.summ_days_release_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_rcm_organization
;
