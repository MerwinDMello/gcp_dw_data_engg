-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_ar_metric_other.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_metric_other AS SELECT
    smry_ar_metric_other.service_type_name,
    smry_ar_metric_other.fact_lvl_code,
    smry_ar_metric_other.parent_code,
    smry_ar_metric_other.child_code,
    smry_ar_metric_other.ytd_month_ind,
    smry_ar_metric_other.month_id,
    smry_ar_metric_other.payor_type,
    smry_ar_metric_other.payor_short_name,
    smry_ar_metric_other.age_group_desc,
    smry_ar_metric_other.payor_fin_class_desc,
    smry_ar_metric_other.account_type_desc,
    smry_ar_metric_other.age_group_member,
    smry_ar_metric_other.scenario_type,
    smry_ar_metric_other.ar_bal_amt,
    smry_ar_metric_other.total_ins_amt,
    smry_ar_metric_other.adj_net_rev_amt,
    smry_ar_metric_other.charity_amt,
    smry_ar_metric_other.gross_revenue_amt,
    smry_ar_metric_other.ins_gt_90_day_amt,
    smry_ar_metric_other.ins_gt_90_day_goal_amt,
    smry_ar_metric_other.uninsured_amt,
    smry_ar_metric_other.bad_debt_amt,
    smry_ar_metric_other.net_ar_amt,
    smry_ar_metric_other.clearing_acct_amt,
    smry_ar_metric_other.days_in_month,
    smry_ar_metric_other.dw_last_update_date_time,
    smry_ar_metric_other.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_ar_metric_other
;
