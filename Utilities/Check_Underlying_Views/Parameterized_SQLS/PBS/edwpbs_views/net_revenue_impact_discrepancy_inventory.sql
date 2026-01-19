-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/net_revenue_impact_discrepancy_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.net_revenue_impact_discrepancy_inventory
   OPTIONS(description='Daily snapshot of Discrepancy Inventory to analyze the impact on Net Revenue')
  AS SELECT
      a.reporting_date,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.eor_log_date,
      a.log_id,
      a.log_sequence_num,
      a.company_code,
      a.coid,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      a.iplan_insurance_order_num,
      a.eff_from_date,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.iplan_id,
      a.remittance_date,
      a.discrepancy_origination_date,
      a.reason_assignment_date_1,
      a.reason_assignment_date_2,
      a.reason_assignment_date_3,
      a.reason_assignment_date_4,
      ROUND(a.over_under_payment_amt, 3, 'ROUND_HALF_EVEN') AS over_under_payment_amt,
      ROUND(a.actual_payment_amt, 3, 'ROUND_HALF_EVEN') AS actual_payment_amt,
      ROUND(a.var_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS var_total_charge_amt,
      ROUND(a.var_gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_reimbursement_amt,
      ROUND(a.var_primary_payor_pay_amt, 3, 'ROUND_HALF_EVEN') AS var_primary_payor_pay_amt,
      ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      a.inpatient_outpatient_code,
      a.discrepancy_reason_code_1,
      a.discrepancy_reason_code_2,
      a.discrepancy_reason_code_3,
      a.discrepancy_reason_code_4,
      a.comment_text,
      a.work_date,
      a.last_racf_id,
      a.last_racf_date,
      a.data_source_code,
      ROUND(a.cc_calc_id, 0, 'ROUND_HALF_EVEN') AS cc_calc_id,
      ROUND(a.cc_account_activity_id, 0, 'ROUND_HALF_EVEN') AS cc_account_activity_id,
      ROUND(a.cc_reason_id, 0, 'ROUND_HALF_EVEN') AS cc_reason_id,
      ROUND(a.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id,
      a.admission_date,
      a.discharge_date,
      ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      a.patient_type_code,
      a.ar_transaction_enter_date,
      a.ar_transaction_effective_date,
      a.take_back_ind,
      a.denial_ind,
      a.payment_type_ind,
      a.cm_transaction_ind,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.net_revenue_impact_discrepancy_inventory AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
