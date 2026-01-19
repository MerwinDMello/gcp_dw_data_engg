-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/address_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.address_pf AS SELECT
    address.address_dw_id,
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
