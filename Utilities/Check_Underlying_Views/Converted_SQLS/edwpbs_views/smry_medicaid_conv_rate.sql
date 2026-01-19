-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_medicaid_conv_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_medicaid_conv_rate AS SELECT
    a.coid,
    a.company_code,
    a.month_id,
    a.patient_type_desc,
    ROUND(a.mc_fc9_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS mc_fc9_total_charge_amt,
    ROUND(a.mcaid_fc3_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS mcaid_fc3_total_charge_amt,
    ROUND(a.mcaid_conv_ttl_charge_amt, 3, 'ROUND_HALF_EVEN') AS mcaid_conv_ttl_charge_amt,
    ROUND(a.self_pay_3_mth_revenue_avg, 3, 'ROUND_HALF_EVEN') AS self_pay_3_mth_revenue_avg,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_medicaid_conv_rate AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON upper(b.co_id) = upper(a.coid)
     AND upper(b.company_code) = upper(a.company_code)
     AND b.user_id = session_user()
;
