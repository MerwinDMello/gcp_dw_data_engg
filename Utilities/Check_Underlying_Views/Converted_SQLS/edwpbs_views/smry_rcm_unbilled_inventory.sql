-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_rcm_unbilled_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_rcm_unbilled_inventory AS SELECT
    a.coid,
    a.company_code,
    a.payor_sid,
    a.patient_type_sid,
    CAST(a.month_id as STRING) AS effective_date,
    a.scenario_sid,
    ROUND(a.in_hse_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS in_hse_acct_bal_amt,
    ROUND(a.lt_summ_days_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS lt_summ_days_acct_bal_amt,
    ROUND(a.gt_summ_days_med_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS gt_summ_days_med_acct_bal_amt,
    ROUND(a.gt_summ_days_bus_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS gt_summ_days_bus_acct_bal_amt,
    ROUND(a.error_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS error_acct_bal_amt,
    ROUND(a.error_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS error_acct_bal_amt_msc,
    ROUND(a.hold_acct_bal_amt_pas, 3, 'ROUND_HALF_EVEN') AS hold_acct_bal_amt_pas,
    ROUND(a.hold_acct_bal_amt_fac, 3, 'ROUND_HALF_EVEN') AS hold_acct_bal_amt_fac,
    ROUND(a.hold_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS hold_acct_bal_amt_msc,
    ROUND(a.hold_acct_bal_amt_final_bill, 3, 'ROUND_HALF_EVEN') AS hold_acct_bal_amt_final_bill,
    ROUND(a.valid_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS valid_acct_bal_amt,
    ROUND(a.valid_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS valid_acct_bal_amt_msc,
    ROUND(a.wait_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS wait_acct_bal_amt,
    ROUND(a.wait_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS wait_acct_bal_amt_msc,
    ROUND(a.add_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS add_acct_bal_amt,
    ROUND(a.add_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS add_acct_bal_amt_msc,
    ROUND(a.gt_summ_days_bus_acct_bal_amt + a.gt_summ_days_med_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_amt,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_rcm_unbilled_inventory AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON upper(b.co_id) = upper(a.coid)
     AND upper(b.company_code) = upper(a.company_code)
     AND b.user_id = session_user()
;
