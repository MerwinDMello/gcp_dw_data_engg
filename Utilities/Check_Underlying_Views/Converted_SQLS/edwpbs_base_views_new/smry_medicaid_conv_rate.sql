-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_medicaid_conv_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_medicaid_conv_rate AS SELECT
    smry_medicaid_conv_rate.coid,
    smry_medicaid_conv_rate.company_code,
    smry_medicaid_conv_rate.month_id,
    smry_medicaid_conv_rate.patient_type_desc,
    smry_medicaid_conv_rate.mc_fc9_total_charge_amt,
    smry_medicaid_conv_rate.mcaid_fc3_total_charge_amt,
    smry_medicaid_conv_rate.mcaid_conv_ttl_charge_amt,
    smry_medicaid_conv_rate.self_pay_3_mth_revenue_avg,
    smry_medicaid_conv_rate.dw_last_update_date_time,
    smry_medicaid_conv_rate.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_medicaid_conv_rate
;
