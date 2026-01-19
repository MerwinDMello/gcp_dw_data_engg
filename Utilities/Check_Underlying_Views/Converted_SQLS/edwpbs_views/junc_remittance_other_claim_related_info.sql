-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/junc_remittance_other_claim_related_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.junc_remittance_other_claim_related_info
   OPTIONS(description='Crosswalk table for Claim Records and Other Claim Related Related Information')
  AS SELECT
      a.claim_guid,
      a.payment_guid,
      ROUND(a.reference_id_line_num, 0, 'ROUND_HALF_EVEN') AS reference_id_line_num,
      ROUND(a.ref_sid, 0, 'ROUND_HALF_EVEN') AS ref_sid,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_remittance_other_claim_related_info AS a
  ;
