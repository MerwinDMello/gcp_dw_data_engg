-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/remittance_claim_carc.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.remittance_claim_carc
   OPTIONS(description='This is the information related to claim level adjustments associated with the claim.')
  AS SELECT
      a.claim_guid,
      a.adj_group_code,
      a.carc_code,
      a.audit_date,
      a.delete_ind,
      a.delete_date,
      a.coid,
      a.company_code,
      ROUND(a.adj_amt, 3, 'ROUND_HALF_EVEN') AS adj_amt,
      a.adj_qty,
      a.adj_category,
      a.cc_adj_group_code,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.remittance_claim_carc AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
