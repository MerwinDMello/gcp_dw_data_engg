-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_rcm_unbilled_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_rcm_unbilled_inventory AS SELECT
    smry_rcm_unbilled_inventory.coid,
    smry_rcm_unbilled_inventory.company_code,
    smry_rcm_unbilled_inventory.payor_sid,
    smry_rcm_unbilled_inventory.patient_type_sid,
    smry_rcm_unbilled_inventory.month_id,
    smry_rcm_unbilled_inventory.scenario_sid,
    ROUND(smry_rcm_unbilled_inventory.in_hse_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS in_hse_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.lt_summ_days_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS lt_summ_days_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.gt_summ_days_med_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS gt_summ_days_med_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.gt_summ_days_bus_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS gt_summ_days_bus_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.error_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS error_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.error_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS error_acct_bal_amt_msc,
    ROUND(smry_rcm_unbilled_inventory.hold_acct_bal_amt_pas, 3, 'ROUND_HALF_EVEN') AS hold_acct_bal_amt_pas,
    ROUND(smry_rcm_unbilled_inventory.hold_acct_bal_amt_fac, 3, 'ROUND_HALF_EVEN') AS hold_acct_bal_amt_fac,
    ROUND(smry_rcm_unbilled_inventory.hold_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS hold_acct_bal_amt_msc,
    ROUND(smry_rcm_unbilled_inventory.hold_acct_bal_amt_final_bill, 3, 'ROUND_HALF_EVEN') AS hold_acct_bal_amt_final_bill,
    ROUND(smry_rcm_unbilled_inventory.valid_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS valid_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.valid_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS valid_acct_bal_amt_msc,
    ROUND(smry_rcm_unbilled_inventory.wait_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS wait_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.wait_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS wait_acct_bal_amt_msc,
    ROUND(smry_rcm_unbilled_inventory.add_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS add_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.add_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS add_acct_bal_amt_msc,
    smry_rcm_unbilled_inventory.dw_last_update_date_time,
    smry_rcm_unbilled_inventory.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_rcm_unbilled_inventory
;
