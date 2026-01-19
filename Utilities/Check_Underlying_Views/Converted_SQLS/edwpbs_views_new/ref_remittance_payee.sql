-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_remittance_payee.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_remittance_payee AS SELECT
    a.remittance_payee_sid,
    a.provider_npi,
    a.provider_tax_id,
    a.provider_tax_id_lookup_code,
    a.payee_name,
    a.payee_identification_qualifier_code,
    a.payee_city_name,
    a.payee_state_code,
    a.payee_postal_zone_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_remittance_payee AS a
;
