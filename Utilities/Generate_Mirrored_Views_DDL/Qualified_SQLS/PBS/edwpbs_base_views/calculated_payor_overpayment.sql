-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/calculated_payor_overpayment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.calculated_payor_overpayment
   OPTIONS(description='This is a daily snapshot of Calculated  Payor Overpayment for the Legacy and Concuity accounts')
  AS SELECT
      ROUND(calculated_payor_overpayment.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      calculated_payor_overpayment.rptg_date,
      ROUND(calculated_payor_overpayment.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      calculated_payor_overpayment.iplan_id,
      calculated_payor_overpayment.coid,
      calculated_payor_overpayment.company_code,
      calculated_payor_overpayment.unit_num,
      calculated_payor_overpayment.iplan_insurance_order_num,
      ROUND(calculated_payor_overpayment.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(calculated_payor_overpayment.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ROUND(calculated_payor_overpayment.patient_person_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_person_dw_id,
      calculated_payor_overpayment.discrepancy_reason_code_1,
      calculated_payor_overpayment.discrepancy_reason_code_3,
      ROUND(calculated_payor_overpayment.cc_project_id, 0, 'ROUND_HALF_EVEN') AS cc_project_id,
      calculated_payor_overpayment.source_sid,
      calculated_payor_overpayment.late_charge_ind,
      calculated_payor_overpayment.drg_change_ind,
      calculated_payor_overpayment.multiple_pmt_ind,
      calculated_payor_overpayment.single_pmt_greater_than_total_chg_ind,
      calculated_payor_overpayment.overpayment_metric_sid,
      ROUND(calculated_payor_overpayment.potential_overpayment_amt, 3, 'ROUND_HALF_EVEN') AS potential_overpayment_amt,
      ROUND(calculated_payor_overpayment.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      calculated_payor_overpayment.source_system_code,
      calculated_payor_overpayment.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.calculated_payor_overpayment
  ;
