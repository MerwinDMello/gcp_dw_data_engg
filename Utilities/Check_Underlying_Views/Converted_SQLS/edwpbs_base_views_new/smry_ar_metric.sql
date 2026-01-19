-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
    smry_ar_metric.bad_debt_wo_amt,
    smry_ar_metric.total_balance_amt,
    smry_ar_metric.net_revenue_amt,
    smry_ar_metric.medicare_ar_day_cnt,
    smry_ar_metric.mc_fc9_total_charge_amt,
    smry_ar_metric.mcaid_fc3_total_charge_amt,
    smry_ar_metric.mcaid_conv_ttl_charge_amt,
    smry_ar_metric.self_pay_3_mth_revenue_avg,
    smry_ar_metric.dw_last_update_date_time,
    smry_ar_metric.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_ar_metric
;
