-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_payee.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_payee
   OPTIONS(description='Reference table to maintain the Payee details of the payments sent.')
  AS SELECT
      ref_remittance_payee.remittance_payee_sid,
      ref_remittance_payee.provider_npi,
      ref_remittance_payee.provider_tax_id,
      ref_remittance_payee.provider_tax_id_lookup_code,
      ref_remittance_payee.payee_name,
      ref_remittance_payee.payee_identification_qualifier_code,
      ref_remittance_payee.payee_city_name,
      ref_remittance_payee.payee_state_code,
      ref_remittance_payee.payee_postal_zone_code,
      ref_remittance_payee.source_system_code,
      ref_remittance_payee.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_payee
  ;
