-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_remittance_other_claim_related_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.junc_remittance_other_claim_related_info
   OPTIONS(description='Crosswalk table for Claim Records and Other Claim Related Related Information')
  AS SELECT
      junc_remittance_other_claim_related_info.claim_guid,
      junc_remittance_other_claim_related_info.payment_guid,
      ROUND(junc_remittance_other_claim_related_info.reference_id_line_num, 0, 'ROUND_HALF_EVEN') AS reference_id_line_num,
      ROUND(junc_remittance_other_claim_related_info.ref_sid, 0, 'ROUND_HALF_EVEN') AS ref_sid,
      junc_remittance_other_claim_related_info.source_system_code,
      junc_remittance_other_claim_related_info.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.junc_remittance_other_claim_related_info
  ;
