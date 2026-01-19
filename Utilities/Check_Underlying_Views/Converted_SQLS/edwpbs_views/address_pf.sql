-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/address_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.address_pf AS SELECT
    ROUND(address_pf.address_dw_id, 0, 'ROUND_HALF_EVEN') AS address_dw_id,
    address_pf.address_1_text,
    address_pf.address_2_text,
    address_pf.city_name,
    address_pf.state_code,
    address_pf.zip_code,
    address_pf.county_code,
    address_pf.country_name,
    address_pf.email_address_text,
    address_pf.city_state_name,
    address_pf.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.address_pf
;
