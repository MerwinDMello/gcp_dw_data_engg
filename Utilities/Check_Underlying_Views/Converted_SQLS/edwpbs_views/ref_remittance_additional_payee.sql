-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_remittance_additional_payee.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_remittance_additional_payee
   OPTIONS(description='Reference table to maintain the Additional Payee details of the payments sent')
  AS SELECT
      a.remittance_additional_payee_sid,
      a.additional_payee_id_qualifier_code,
      a.additional_payee_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_remittance_additional_payee AS a
  ;
