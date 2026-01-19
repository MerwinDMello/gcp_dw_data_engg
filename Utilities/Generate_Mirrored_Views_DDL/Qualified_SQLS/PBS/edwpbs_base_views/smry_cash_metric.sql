-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_cash_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.smry_cash_metric AS SELECT
    smry_cash_metric.service_type_name,
    smry_cash_metric.fact_lvl_code,
    smry_cash_metric.parent_code,
    smry_cash_metric.child_code,
    smry_cash_metric.ytd_month_ind,
    smry_cash_metric.month_id,
    smry_cash_metric.payor_short_name,
    smry_cash_metric.ufc_pmt_type,
    smry_cash_metric.patient_type,
    ROUND(smry_cash_metric.cash_amt, 3, 'ROUND_HALF_EVEN') AS cash_amt,
    ROUND(smry_cash_metric.fm_cash_amt, 3, 'ROUND_HALF_EVEN') AS fm_cash_amt,
    ROUND(smry_cash_metric.adj_net_rev_amt, 3, 'ROUND_HALF_EVEN') AS adj_net_rev_amt,
    ROUND(smry_cash_metric.upfront_coll_amt, 3, 'ROUND_HALF_EVEN') AS upfront_coll_amt,
    ROUND(smry_cash_metric.net_revenue_amt, 3, 'ROUND_HALF_EVEN') AS net_revenue_amt,
    ROUND(smry_cash_metric.net_revenue_2m_prior_amt, 3, 'ROUND_HALF_EVEN') AS net_revenue_2m_prior_amt,
    ROUND(smry_cash_metric.gross_revenue_amt, 3, 'ROUND_HALF_EVEN') AS gross_revenue_amt,
    ROUND(smry_cash_metric.gt_smry_day_bus_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS gt_smry_day_bus_acct_bal_amt,
    ROUND(smry_cash_metric.gt_smry_day_med_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS gt_smry_day_med_acct_bal_amt,
    ROUND(smry_cash_metric.total_ssi_amt, 3, 'ROUND_HALF_EVEN') AS total_ssi_amt,
    smry_cash_metric.bank_days,
    smry_cash_metric.days_in_month,
    smry_cash_metric.dw_last_update_date_time,
    smry_cash_metric.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.smry_cash_metric
;
