-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_cash_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.smry_cash_metric AS SELECT
    max(b.service_type_name) AS service_type_name,
    c.fact_lvl_code,
    max(c.parent_code) AS parent_code,
    max(c.child_code) AS child_code,
    max(a.ytd_month_ind) AS ytd_month_ind,
    a.month_id,
    max(a.payor_short_name) AS payor_short_name,
    max(a.ufc_pmt_type) AS ufc_pmt_type,
    max(a.patient_type) AS patient_type,
    sum(a.cash_amt) AS cash_amt,
    sum(a.fm_cash_amt) AS fm_cash_amt,
    sum(a.adj_net_rev_amt) AS adj_net_rev_amt,
    sum(a.upfront_coll_amt) AS upfront_coll_amt,
    sum(a.net_revenue_amt) AS net_revenue_amt,
    sum(a.net_revenue_2m_prior_amt) AS net_revenue_2m_prior_amt,
    sum(a.gross_revenue_amt) AS gross_revenue_amt,
    sum(a.gt_smry_day_bus_acct_bal_amt) AS gt_smry_day_bus_acct_bal_amt,
    sum(a.gt_smry_day_med_acct_bal_amt) AS gt_smry_day_med_acct_bal_amt,
    sum(a.total_ssi_amt) AS total_ssi_amt,
    max(a.bank_days) AS bank_days,
    max(a.days_in_month) AS days_in_month
  FROM
    {{ params.param_pbs_base_views_dataset_name }}.smry_cash_metric AS a
    INNER JOIN {{ params.param_pbs_views_dataset_name }}.lu_rcm_level_security AS b ON upper(a.parent_code) = upper(b.coid)
    INNER JOIN {{ params.param_pbs_views_dataset_name }}.dim_rcm_org_level AS c ON upper(b.service_type_name) = upper(c.service_type_name)
     AND upper(b.parent_code) = upper(c.child_code)
  GROUP BY upper(b.service_type_name), 2, upper(c.parent_code), upper(c.child_code), upper(a.ytd_month_ind), 6, upper(a.payor_short_name), upper(a.ufc_pmt_type), upper(a.patient_type)
;
