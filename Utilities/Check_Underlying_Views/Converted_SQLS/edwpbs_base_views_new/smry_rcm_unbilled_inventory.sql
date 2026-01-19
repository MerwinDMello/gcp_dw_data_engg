-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
    smry_rcm_unbilled_inventory.in_hse_acct_bal_amt,
    smry_rcm_unbilled_inventory.lt_summ_days_acct_bal_amt,
    smry_rcm_unbilled_inventory.gt_summ_days_med_acct_bal_amt,
    smry_rcm_unbilled_inventory.gt_summ_days_bus_acct_bal_amt,
    smry_rcm_unbilled_inventory.error_acct_bal_amt,
    smry_rcm_unbilled_inventory.error_acct_bal_amt_msc,
    smry_rcm_unbilled_inventory.hold_acct_bal_amt_pas,
    smry_rcm_unbilled_inventory.hold_acct_bal_amt_fac,
    smry_rcm_unbilled_inventory.hold_acct_bal_amt_msc,
    smry_rcm_unbilled_inventory.hold_acct_bal_amt_final_bill,
    smry_rcm_unbilled_inventory.valid_acct_bal_amt,
    smry_rcm_unbilled_inventory.valid_acct_bal_amt_msc,
    smry_rcm_unbilled_inventory.wait_acct_bal_amt,
    smry_rcm_unbilled_inventory.wait_acct_bal_amt_msc,
    smry_rcm_unbilled_inventory.add_acct_bal_amt,
    smry_rcm_unbilled_inventory.add_acct_bal_amt_msc,
    smry_rcm_unbilled_inventory.dw_last_update_date_time,
    smry_rcm_unbilled_inventory.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_rcm_unbilled_inventory
;
