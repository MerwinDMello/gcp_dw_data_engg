-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_rcm_mc_discrepancy_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.smry_rcm_mc_discrepancy_rate AS SELECT
    a.coid,
    a.company_code,
    a.month_id,
    ROUND(a.over_under_amt, 3, 'ROUND_HALF_EVEN') AS over_under_amt,
    ROUND(a.exp_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS exp_reimbursement_amt,
    ROUND(a.discrepancy_rate, 3, 'ROUND_HALF_EVEN') AS discrepancy_rate,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    {{ params.param_pbs_base_views_dataset_name }}.smry_rcm_mc_discrepancy_rate AS a
    CROSS JOIN {{ params.param_pbs_base_views_dataset_name }}.secref_facility AS b
  WHERE upper(b.co_id) = upper(a.coid)
   AND upper(b.company_code) = upper(a.company_code)
   AND b.user_id = session_user()
;
