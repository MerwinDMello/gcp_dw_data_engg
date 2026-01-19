-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_cash_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_cash_metric AS SELECT
    smry_cash_metric.service_type_name,
    smry_cash_metric.fact_lvl_code,
    smry_cash_metric.parent_code,
    smry_cash_metric.child_code,
    smry_cash_metric.ytd_month_ind,
    smry_cash_metric.month_id,
    smry_cash_metric.payor_short_name,
    smry_cash_metric.ufc_pmt_type,
    smry_cash_metric.patient_type,
    smry_cash_metric.cash_amt,
    smry_cash_metric.fm_cash_amt,
    smry_cash_metric.adj_net_rev_amt,
    smry_cash_metric.upfront_coll_amt,
    smry_cash_metric.net_revenue_amt,
    smry_cash_metric.net_revenue_2m_prior_amt,
    smry_cash_metric.gross_revenue_amt,
    smry_cash_metric.gt_smry_day_bus_acct_bal_amt,
    smry_cash_metric.gt_smry_day_med_acct_bal_amt,
    smry_cash_metric.total_ssi_amt,
    smry_cash_metric.bank_days,
    smry_cash_metric.days_in_month,
    smry_cash_metric.dw_last_update_date_time,
    smry_cash_metric.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_cash_metric
;
