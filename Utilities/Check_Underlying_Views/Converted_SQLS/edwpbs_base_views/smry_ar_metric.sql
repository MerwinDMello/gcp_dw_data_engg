-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_ar_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_metric AS SELECT
    smry_ar_metric.service_type_name,
    smry_ar_metric.fact_lvl_code,
    smry_ar_metric.parent_code,
    smry_ar_metric.child_code,
    smry_ar_metric.ytd_month_ind,
    smry_ar_metric.month_id,
    smry_ar_metric.patient_type_desc,
    ROUND(smry_ar_metric.bad_debt_wo_amt, 3, 'ROUND_HALF_EVEN') AS bad_debt_wo_amt,
    ROUND(smry_ar_metric.total_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_balance_amt,
    ROUND(smry_ar_metric.net_revenue_amt, 3, 'ROUND_HALF_EVEN') AS net_revenue_amt,
    ROUND(smry_ar_metric.medicare_ar_day_cnt, 3, 'ROUND_HALF_EVEN') AS medicare_ar_day_cnt,
    ROUND(smry_ar_metric.mc_fc9_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS mc_fc9_total_charge_amt,
    ROUND(smry_ar_metric.mcaid_fc3_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS mcaid_fc3_total_charge_amt,
    ROUND(smry_ar_metric.mcaid_conv_ttl_charge_amt, 3, 'ROUND_HALF_EVEN') AS mcaid_conv_ttl_charge_amt,
    ROUND(smry_ar_metric.self_pay_3_mth_revenue_avg, 3, 'ROUND_HALF_EVEN') AS self_pay_3_mth_revenue_avg,
    smry_ar_metric.dw_last_update_date_time,
    smry_ar_metric.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_ar_metric
;
