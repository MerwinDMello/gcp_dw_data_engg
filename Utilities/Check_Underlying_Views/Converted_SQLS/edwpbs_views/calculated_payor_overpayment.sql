-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/calculated_payor_overpayment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.calculated_payor_overpayment
   OPTIONS(description='This is a daily snapshot of Calculated  Payor Overpayment for the Legacy and Concuity accounts')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.rptg_date,
      ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      a.iplan_id,
      a.coid,
      a.company_code,
      a.unit_num,
      a.iplan_insurance_order_num,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ROUND(a.patient_person_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_person_dw_id,
      a.discrepancy_reason_code_1,
      a.discrepancy_reason_code_3,
      ROUND(a.cc_project_id, 0, 'ROUND_HALF_EVEN') AS cc_project_id,
      a.source_sid,
      a.late_charge_ind,
      a.drg_change_ind,
      a.multiple_pmt_ind,
      a.single_pmt_greater_than_total_chg_ind,
      a.overpayment_metric_sid,
      ROUND(a.potential_overpayment_amt, 3, 'ROUND_HALF_EVEN') AS potential_overpayment_amt,
      ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      a.source_system_code,
      a.dw_last_update_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.calculated_payor_overpayment AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
