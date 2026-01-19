-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_ar_metric_other.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.smry_ar_metric_other AS SELECT
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
    ROUND(smry_ar_metric_other.ar_bal_amt, 3, 'ROUND_HALF_EVEN') AS ar_bal_amt,
    ROUND(smry_ar_metric_other.total_ins_amt, 3, 'ROUND_HALF_EVEN') AS total_ins_amt,
    ROUND(smry_ar_metric_other.adj_net_rev_amt, 3, 'ROUND_HALF_EVEN') AS adj_net_rev_amt,
    ROUND(smry_ar_metric_other.charity_amt, 3, 'ROUND_HALF_EVEN') AS charity_amt,
    ROUND(smry_ar_metric_other.gross_revenue_amt, 3, 'ROUND_HALF_EVEN') AS gross_revenue_amt,
    ROUND(smry_ar_metric_other.ins_gt_90_day_amt, 3, 'ROUND_HALF_EVEN') AS ins_gt_90_day_amt,
    ROUND(smry_ar_metric_other.ins_gt_90_day_goal_amt, 3, 'ROUND_HALF_EVEN') AS ins_gt_90_day_goal_amt,
    ROUND(smry_ar_metric_other.uninsured_amt, 3, 'ROUND_HALF_EVEN') AS uninsured_amt,
    ROUND(smry_ar_metric_other.bad_debt_amt, 3, 'ROUND_HALF_EVEN') AS bad_debt_amt,
    ROUND(smry_ar_metric_other.net_ar_amt, 3, 'ROUND_HALF_EVEN') AS net_ar_amt,
    ROUND(smry_ar_metric_other.clearing_acct_amt, 3, 'ROUND_HALF_EVEN') AS clearing_acct_amt,
    smry_ar_metric_other.days_in_month,
    smry_ar_metric_other.dw_last_update_date_time,
    smry_ar_metric_other.source_system_code
  FROM
    {{ params.param_pbs_core_dataset_name }}.smry_ar_metric_other
;
