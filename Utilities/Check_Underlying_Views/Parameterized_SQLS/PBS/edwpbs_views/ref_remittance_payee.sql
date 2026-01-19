-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_remittance_payee.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.ref_remittance_payee
   OPTIONS(description='Reference table to maintain the Payee details of the payments sent.')
  AS SELECT
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
      {{ params.param_pbs_base_views_dataset_name }}.ref_remittance_payee AS a
  ;
