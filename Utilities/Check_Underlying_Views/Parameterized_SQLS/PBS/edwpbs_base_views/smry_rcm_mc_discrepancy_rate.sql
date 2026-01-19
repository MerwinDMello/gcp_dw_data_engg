-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_rcm_mc_discrepancy_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.smry_rcm_mc_discrepancy_rate AS SELECT
    smry_rcm_mc_discrepancy_rate.coid,
    smry_rcm_mc_discrepancy_rate.company_code,
    smry_rcm_mc_discrepancy_rate.month_id,
    ROUND(smry_rcm_mc_discrepancy_rate.over_under_amt, 3, 'ROUND_HALF_EVEN') AS over_under_amt,
    ROUND(smry_rcm_mc_discrepancy_rate.exp_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS exp_reimbursement_amt,
    ROUND(smry_rcm_mc_discrepancy_rate.discrepancy_rate, 3, 'ROUND_HALF_EVEN') AS discrepancy_rate,
    smry_rcm_mc_discrepancy_rate.dw_last_update_date_time,
    smry_rcm_mc_discrepancy_rate.source_system_code
  FROM
    {{ params.param_pbs_core_dataset_name }}.smry_rcm_mc_discrepancy_rate
;
