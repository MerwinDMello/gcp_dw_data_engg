-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_pc_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.smry_pc_metric AS SELECT
    max(b.service_type_name) AS service_type_name,
    c.fact_lvl_code,
    max(c.parent_code) AS parent_code,
    max(c.child_code) AS child_code,
    max(a.ytd_month_ind) AS ytd_month_ind,
    a.month_id,
    max(a.payor_short_name) AS payor_short_name,
    max(a.denial_short_desc) AS denial_short_desc,
    sum(a.unpay_inv_amt) AS unpay_inv_amt,
    sum(a.unpay_reason_inv_amt) AS unpay_reason_inv_amt,
    sum(a.refund_amt) AS refund_amt,
    sum(a.credit_balance_amt) AS credit_balance_amt,
    sum(a.ending_balance_amt) AS ending_balance_amt,
    sum(a.wo_denial_amt) AS wo_denial_amt
  FROM
    {{ params.param_pbs_base_views_dataset_name }}.smry_pc_metric AS a
    INNER JOIN {{ params.param_pbs_views_dataset_name }}.lu_rcm_level_security AS b ON upper(a.parent_code) = upper(b.coid)
    INNER JOIN {{ params.param_pbs_views_dataset_name }}.dim_rcm_org_level AS c ON upper(b.service_type_name) = upper(c.service_type_name)
     AND upper(b.parent_code) = upper(c.child_code)
  GROUP BY upper(b.service_type_name), 2, upper(c.parent_code), upper(c.child_code), upper(a.ytd_month_ind), 6, upper(a.payor_short_name), upper(a.denial_short_desc)
;
