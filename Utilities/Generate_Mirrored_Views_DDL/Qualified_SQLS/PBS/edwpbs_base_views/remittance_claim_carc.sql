-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_claim_carc.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.remittance_claim_carc
   OPTIONS(description='This is the information related to claim level adjustments associated with the claim.')
  AS SELECT
      remittance_claim_carc.claim_guid,
      remittance_claim_carc.adj_group_code,
      remittance_claim_carc.carc_code,
      remittance_claim_carc.audit_date,
      remittance_claim_carc.delete_ind,
      remittance_claim_carc.delete_date,
      remittance_claim_carc.coid,
      remittance_claim_carc.company_code,
      ROUND(remittance_claim_carc.adj_amt, 3, 'ROUND_HALF_EVEN') AS adj_amt,
      remittance_claim_carc.adj_qty,
      remittance_claim_carc.adj_category,
      remittance_claim_carc.cc_adj_group_code,
      remittance_claim_carc.dw_last_update_date_time,
      remittance_claim_carc.source_system_code
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.remittance_claim_carc
  ;
