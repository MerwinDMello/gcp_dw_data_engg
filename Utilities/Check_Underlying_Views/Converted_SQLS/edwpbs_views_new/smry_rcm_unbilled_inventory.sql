-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_rcm_unbilled_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_rcm_unbilled_inventory AS SELECT
    a.coid,
    a.company_code,
    a.payor_sid,
    a.patient_type_sid,
    CAST(/* expression of unknown or erroneous type */ a.month_id as STRING) AS effective_date,
    a.scenario_sid,
    a.in_hse_acct_bal_amt,
    a.lt_summ_days_acct_bal_amt,
    a.gt_summ_days_med_acct_bal_amt,
    a.gt_summ_days_bus_acct_bal_amt,
    a.error_acct_bal_amt,
    a.error_acct_bal_amt_msc,
    a.hold_acct_bal_amt_pas,
    a.hold_acct_bal_amt_fac,
    a.hold_acct_bal_amt_msc,
    a.hold_acct_bal_amt_final_bill,
    a.valid_acct_bal_amt,
    a.valid_acct_bal_amt_msc,
    a.wait_acct_bal_amt,
    a.wait_acct_bal_amt_msc,
    a.add_acct_bal_amt,
    a.add_acct_bal_amt_msc,
    a.gt_summ_days_bus_acct_bal_amt + a.gt_summ_days_med_acct_bal_amt AS unbilled_amt,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_rcm_unbilled_inventory AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON b.co_id = a.coid
     AND b.company_code = a.company_code
     AND b.user_id = session_user()
;
