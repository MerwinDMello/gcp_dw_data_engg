-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_pc_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_pc_metric AS SELECT
    max(b.service_type_name) AS service_type_name,
    c.fact_lvl_code,
    max(c.parent_code) AS parent_code,
    max(c.child_code) AS child_code,
    a.ytd_month_ind,
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
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_pc_metric AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_level_security AS b ON a.parent_code = b.coid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_org_level AS c ON upper(b.service_type_name) = upper(c.service_type_name)
     AND b.parent_code = c.child_code
  GROUP BY upper(b.service_type_name), 2, upper(c.parent_code), upper(c.child_code), 5, 6, upper(a.payor_short_name), upper(a.denial_short_desc)
;
