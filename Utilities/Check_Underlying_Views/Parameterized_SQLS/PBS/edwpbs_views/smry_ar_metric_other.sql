-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_ar_metric_other.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.smry_ar_metric_other AS SELECT
    max(b.service_type_name) AS service_type_name,
    c.fact_lvl_code,
    max(c.parent_code) AS parent_code,
    max(c.child_code) AS child_code,
    max(a.ytd_month_ind) AS ytd_month_ind,
    a.month_id,
    max(a.payor_type) AS payor_type,
    max(a.payor_short_name) AS payor_short_name,
    max(a.age_group_desc) AS age_group_desc,
    max(a.payor_fin_class_desc) AS payor_fin_class_desc,
    max(a.account_type_desc) AS account_type_desc,
    max(a.age_group_member) AS age_group_member,
    max(a.scenario_type) AS scenario_type,
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
    {{ params.param_pbs_base_views_dataset_name }}.smry_ar_metric_other AS a
    INNER JOIN {{ params.param_pbs_views_dataset_name }}.lu_rcm_level_security AS b ON upper(a.parent_code) = upper(b.coid)
    INNER JOIN {{ params.param_pbs_views_dataset_name }}.dim_rcm_org_level AS c ON upper(b.service_type_name) = upper(c.service_type_name)
     AND upper(b.parent_code) = upper(c.child_code)
  GROUP BY upper(b.service_type_name), 2, upper(c.parent_code), upper(c.child_code), upper(a.ytd_month_ind), 6, upper(a.payor_type), upper(a.payor_short_name), upper(a.age_group_desc), upper(a.payor_fin_class_desc), upper(a.account_type_desc), upper(a.age_group_member), upper(a.scenario_type)
;
