-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_cash_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_cash_metric AS SELECT
    max(b.service_type_name) AS service_type_name,
    c.fact_lvl_code,
    max(c.parent_code) AS parent_code,
    max(c.child_code) AS child_code,
    a.ytd_month_ind,
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
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_cash_metric AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_level_security AS b ON a.parent_code = b.coid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_org_level AS c ON upper(b.service_type_name) = upper(c.service_type_name)
     AND b.parent_code = c.child_code
  GROUP BY upper(b.service_type_name), 2, upper(c.parent_code), upper(c.child_code), 5, 6, upper(a.payor_short_name), upper(a.ufc_pmt_type), upper(a.patient_type)
;
