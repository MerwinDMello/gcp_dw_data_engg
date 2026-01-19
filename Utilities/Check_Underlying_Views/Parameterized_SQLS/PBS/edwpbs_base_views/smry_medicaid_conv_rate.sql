-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_medicaid_conv_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.smry_medicaid_conv_rate AS SELECT
    smry_medicaid_conv_rate.coid,
    smry_medicaid_conv_rate.company_code,
    smry_medicaid_conv_rate.month_id,
    smry_medicaid_conv_rate.patient_type_desc,
    ROUND(smry_medicaid_conv_rate.mc_fc9_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS mc_fc9_total_charge_amt,
    ROUND(smry_medicaid_conv_rate.mcaid_fc3_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS mcaid_fc3_total_charge_amt,
    ROUND(smry_medicaid_conv_rate.mcaid_conv_ttl_charge_amt, 3, 'ROUND_HALF_EVEN') AS mcaid_conv_ttl_charge_amt,
    ROUND(smry_medicaid_conv_rate.self_pay_3_mth_revenue_avg, 3, 'ROUND_HALF_EVEN') AS self_pay_3_mth_revenue_avg,
    smry_medicaid_conv_rate.dw_last_update_date_time,
    smry_medicaid_conv_rate.source_system_code
  FROM
    {{ params.param_pbs_core_dataset_name }}.smry_medicaid_conv_rate
;
