-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/junc_remittance_delete.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.junc_remittance_delete
   OPTIONS(description='Crosswalk table for Payment Records to be deleted.')
  AS SELECT
      a.check_num_an,
      a.check_date,
      ROUND(a.check_amt, 3, 'ROUND_HALF_EVEN') AS check_amt,
      a.interchange_sender_id,
      a.provider_adjustment_id,
      a.payment_guid,
      a.claim_guid,
      a.service_guid,
      a.delete_date,
      a.coid,
      a.unit_num,
      a.company_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_remittance_delete AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
