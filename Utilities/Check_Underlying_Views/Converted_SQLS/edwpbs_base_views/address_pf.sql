-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/address_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.address_pf AS SELECT
    ROUND(address.address_dw_id, 0, 'ROUND_HALF_EVEN') AS address_dw_id,
    address.address_1_text,
    address.address_2_text,
    address.city_name,
    address.state_code,
    address.zip_code,
    address.county_code,
    address.country_name,
    address.email_address_text,
    address.city_state_name,
    address.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.address
;
