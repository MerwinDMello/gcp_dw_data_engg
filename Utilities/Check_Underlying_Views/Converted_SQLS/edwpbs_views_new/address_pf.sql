-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/address_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.address_pf AS SELECT
    address_pf.address_dw_id,
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
