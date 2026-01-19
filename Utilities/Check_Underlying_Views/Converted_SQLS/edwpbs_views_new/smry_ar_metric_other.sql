-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_ar_metric_other.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_ar_metric_other AS SELECT
    max(b.service_type_name) AS service_type_name,
    c.fact_lvl_code,
    max(c.parent_code) AS parent_code,
    max(c.child_code) AS child_code,
    a.ytd_month_ind,
    a.month_id,
    a.payor_type,
    a.payor_short_name,
    a.age_group_desc,
    a.payor_fin_class_desc,
    a.account_type_desc,
    a.age_group_member,
    a.scenario_type,
    sum(a.ar_bal_amt) AS ar_bal_amt,
    sum(a.total_ins_amt) AS total_ins_amt,
    sum(a.adj_net_rev_amt) AS adj_net_rev_amt,
    sum(a.charity_amt) AS charity_amt,
    sum(a.gross_revenue_amt) AS gross_revenue_amt,
    sum(a.uninsured_amt) AS uninsured_amt,
    sum(a.bad_debt_amt) AS bad_debt_amt,
    sum(a.net_ar_amt) AS net_ar_amt,
    sum(a.clearing_acct_amt) AS clearing_acct_amt,
    max(a.days_in_month) AS days_in_month
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_metric_other AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_level_security AS b ON a.parent_code = b.coid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_org_level AS c ON upper(b.service_type_name) = upper(c.service_type_name)
     AND b.parent_code = c.child_code
  GROUP BY upper(b.service_type_name), 2, upper(c.parent_code), upper(c.child_code), 5, 6, 7, 8, 9, 10, 11, 12, 13
;
