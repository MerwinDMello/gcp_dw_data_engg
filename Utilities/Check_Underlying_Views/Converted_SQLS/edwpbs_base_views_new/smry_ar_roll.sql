-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_ar_roll.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_roll AS SELECT
    smry_ar_roll.unit_num,
    smry_ar_roll.coid,
    smry_ar_roll.company_code,
    smry_ar_roll.pe_date,
    smry_ar_roll.revenue_amt,
    smry_ar_roll.bad_debt_wo_amt,
    smry_ar_roll.charity_wo_allow_amt,
    smry_ar_roll.charity_wo_adj_amt,
    smry_ar_roll.discharged_billed_amt,
    smry_ar_roll.discharged_prelim_amt,
    smry_ar_roll.discharged_unbilled_amt,
    smry_ar_roll.gov_denial_wo_amt,
    smry_ar_roll.inhouse_unbilled_amt,
    smry_ar_roll.insurance_refund_amt,
    smry_ar_roll.non_gov_denial_wo_amt,
    smry_ar_roll.overturned_denial_amt,
    smry_ar_roll.patient_payment_amt,
    smry_ar_roll.patient_refund_amt,
    smry_ar_roll.small_bal_wo_amt,
    smry_ar_roll.insurance_payment_amt,
    smry_ar_roll.true_up_impact_amt,
    smry_ar_roll.adjustment_amt,
    smry_ar_roll.allowance_amt,
    smry_ar_roll.uninsured_discount_amt,
    smry_ar_roll.underpayment_recovery_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.smry_ar_roll
;
