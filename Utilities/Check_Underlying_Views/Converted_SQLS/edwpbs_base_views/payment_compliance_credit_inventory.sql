-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_compliance_credit_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_compliance_credit_inventory
   OPTIONS(description='This is a daily table that contains Credit Inventory of all the accounts showing Refund amounts and Credit Balance Amounts and other details.')
  AS SELECT
      ROUND(payment_compliance_credit_inventory.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      payment_compliance_credit_inventory.reporting_date,
      payment_compliance_credit_inventory.credit_balance_refund_ind,
      payment_compliance_credit_inventory.credit_balance_refund_id,
      payment_compliance_credit_inventory.company_code,
      payment_compliance_credit_inventory.coid,
      ROUND(payment_compliance_credit_inventory.payor_dw_id_ins1, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins1,
      payment_compliance_credit_inventory.unit_num,
      ROUND(payment_compliance_credit_inventory.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      payment_compliance_credit_inventory.admission_type_code,
      payment_compliance_credit_inventory.account_status_code,
      payment_compliance_credit_inventory.refund_type_sid,
      payment_compliance_credit_inventory.credit_status_sid,
      ROUND(payment_compliance_credit_inventory.payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_code,
      payment_compliance_credit_inventory.iplan_id_ins1,
      payment_compliance_credit_inventory.iplan_id_ins2,
      payment_compliance_credit_inventory.iplan_id_ins3,
      ROUND(payment_compliance_credit_inventory.patient_person_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_person_dw_id,
      ROUND(payment_compliance_credit_inventory.patient_address_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_address_dw_id,
      payment_compliance_credit_inventory.member_id,
      payment_compliance_credit_inventory.logged_ind,
      payment_compliance_credit_inventory.refund_iplan_id,
      payment_compliance_credit_inventory.refund_special_code,
      payment_compliance_credit_inventory.refund_procedure_code,
      payment_compliance_credit_inventory.refund_gl_acct_num,
      ROUND(payment_compliance_credit_inventory.refund_payor_dw_id, 0, 'ROUND_HALF_EVEN') AS refund_payor_dw_id,
      ROUND(payment_compliance_credit_inventory.refund_payor_address_dw_id, 0, 'ROUND_HALF_EVEN') AS refund_payor_address_dw_id,
      payment_compliance_credit_inventory.refund_creation_date_time,
      payment_compliance_credit_inventory.refund_creation_user_id,
      ROUND(payment_compliance_credit_inventory.refund_amt, 3, 'ROUND_HALF_EVEN') AS refund_amt,
      ROUND(payment_compliance_credit_inventory.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(payment_compliance_credit_inventory.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(payment_compliance_credit_inventory.total_cash_pay_amt, 3, 'ROUND_HALF_EVEN') AS total_cash_pay_amt,
      ROUND(payment_compliance_credit_inventory.total_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_allow_amt,
      ROUND(payment_compliance_credit_inventory.total_policy_adj_amt, 3, 'ROUND_HALF_EVEN') AS total_policy_adj_amt,
      ROUND(payment_compliance_credit_inventory.total_write_off_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_amt,
      payment_compliance_credit_inventory.credit_balance_date,
      ROUND(payment_compliance_credit_inventory.current_grv_amt, 3, 'ROUND_HALF_EVEN') AS current_grv_amt,
      payment_compliance_credit_inventory.entered_date,
      payment_compliance_credit_inventory.discharge_date,
      payment_compliance_credit_inventory.admission_date,
      payment_compliance_credit_inventory.final_bill_date,
      payment_compliance_credit_inventory.bill_through_date,
      payment_compliance_credit_inventory.credit_bal_estb_date_time,
      payment_compliance_credit_inventory.last_update_date_time,
      payment_compliance_credit_inventory.last_update_user_id,
      payment_compliance_credit_inventory.approved_by_user_id,
      payment_compliance_credit_inventory.approved_date_time,
      payment_compliance_credit_inventory.auto_refund_create_date_time,
      payment_compliance_credit_inventory.resolved_date,
      payment_compliance_credit_inventory.eligible_transfer_ind,
      payment_compliance_credit_inventory.late_charge_ind,
      payment_compliance_credit_inventory.restore_date_time,
      payment_compliance_credit_inventory.restore_ind,
      payment_compliance_credit_inventory.source_system_code,
      payment_compliance_credit_inventory.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.payment_compliance_credit_inventory
  ;
