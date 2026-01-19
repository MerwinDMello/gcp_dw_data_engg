-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_discrepancy_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.net_revenue_impact_discrepancy_inventory
   OPTIONS(description='Daily snapshot of Discrepancy Inventory to analyze the impact on Net Revenue')
  AS SELECT
      net_revenue_impact_discrepancy_inventory.reporting_date,
      ROUND(net_revenue_impact_discrepancy_inventory.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      net_revenue_impact_discrepancy_inventory.eor_log_date,
      net_revenue_impact_discrepancy_inventory.log_id,
      net_revenue_impact_discrepancy_inventory.log_sequence_num,
      net_revenue_impact_discrepancy_inventory.company_code,
      net_revenue_impact_discrepancy_inventory.coid,
      ROUND(net_revenue_impact_discrepancy_inventory.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      net_revenue_impact_discrepancy_inventory.iplan_insurance_order_num,
      net_revenue_impact_discrepancy_inventory.eff_from_date,
      ROUND(net_revenue_impact_discrepancy_inventory.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      net_revenue_impact_discrepancy_inventory.iplan_id,
      net_revenue_impact_discrepancy_inventory.remittance_date,
      net_revenue_impact_discrepancy_inventory.discrepancy_origination_date,
      net_revenue_impact_discrepancy_inventory.reason_assignment_date_1,
      net_revenue_impact_discrepancy_inventory.reason_assignment_date_2,
      net_revenue_impact_discrepancy_inventory.reason_assignment_date_3,
      net_revenue_impact_discrepancy_inventory.reason_assignment_date_4,
      ROUND(net_revenue_impact_discrepancy_inventory.over_under_payment_amt, 3, 'ROUND_HALF_EVEN') AS over_under_payment_amt,
      ROUND(net_revenue_impact_discrepancy_inventory.actual_payment_amt, 3, 'ROUND_HALF_EVEN') AS actual_payment_amt,
      ROUND(net_revenue_impact_discrepancy_inventory.var_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS var_total_charge_amt,
      ROUND(net_revenue_impact_discrepancy_inventory.var_gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_reimbursement_amt,
      ROUND(net_revenue_impact_discrepancy_inventory.var_primary_payor_pay_amt, 3, 'ROUND_HALF_EVEN') AS var_primary_payor_pay_amt,
      ROUND(net_revenue_impact_discrepancy_inventory.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      net_revenue_impact_discrepancy_inventory.inpatient_outpatient_code,
      net_revenue_impact_discrepancy_inventory.discrepancy_reason_code_1,
      net_revenue_impact_discrepancy_inventory.discrepancy_reason_code_2,
      net_revenue_impact_discrepancy_inventory.discrepancy_reason_code_3,
      net_revenue_impact_discrepancy_inventory.discrepancy_reason_code_4,
      net_revenue_impact_discrepancy_inventory.comment_text,
      net_revenue_impact_discrepancy_inventory.work_date,
      net_revenue_impact_discrepancy_inventory.last_racf_id,
      net_revenue_impact_discrepancy_inventory.last_racf_date,
      net_revenue_impact_discrepancy_inventory.data_source_code,
      ROUND(net_revenue_impact_discrepancy_inventory.cc_calc_id, 0, 'ROUND_HALF_EVEN') AS cc_calc_id,
      ROUND(net_revenue_impact_discrepancy_inventory.cc_account_activity_id, 0, 'ROUND_HALF_EVEN') AS cc_account_activity_id,
      ROUND(net_revenue_impact_discrepancy_inventory.cc_reason_id, 0, 'ROUND_HALF_EVEN') AS cc_reason_id,
      ROUND(net_revenue_impact_discrepancy_inventory.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id,
      net_revenue_impact_discrepancy_inventory.admission_date,
      net_revenue_impact_discrepancy_inventory.discharge_date,
      ROUND(net_revenue_impact_discrepancy_inventory.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      net_revenue_impact_discrepancy_inventory.patient_type_code,
      net_revenue_impact_discrepancy_inventory.ar_transaction_enter_date,
      net_revenue_impact_discrepancy_inventory.ar_transaction_effective_date,
      net_revenue_impact_discrepancy_inventory.take_back_ind,
      net_revenue_impact_discrepancy_inventory.denial_ind,
      net_revenue_impact_discrepancy_inventory.payment_type_ind,
      net_revenue_impact_discrepancy_inventory.cm_transaction_ind,
      net_revenue_impact_discrepancy_inventory.dw_last_update_date_time,
      net_revenue_impact_discrepancy_inventory.source_system_code
    FROM
      {{ params.param_pbs_core_dataset_name }}.net_revenue_impact_discrepancy_inventory
  ;
