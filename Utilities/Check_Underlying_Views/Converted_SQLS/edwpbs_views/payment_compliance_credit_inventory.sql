-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payment_compliance_credit_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_credit_inventory
   OPTIONS(description='This is a daily table that contains Credit Inventory of all the accounts showing Refund amounts and Credit Balance Amounts and other details.')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.reporting_date,
      a.credit_balance_refund_ind,
      a.credit_balance_refund_id,
      a.company_code,
      a.coid,
      ROUND(a.payor_dw_id_ins1, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins1,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.admission_type_code,
      a.account_status_code,
      a.refund_type_sid,
      a.credit_status_sid,
      ROUND(a.payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_code,
      a.iplan_id_ins1,
      a.iplan_id_ins2,
      a.iplan_id_ins3,
      ROUND(a.patient_person_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_person_dw_id,
      ROUND(a.patient_address_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_address_dw_id,
      a.member_id,
      a.logged_ind,
      a.refund_iplan_id,
      a.refund_special_code,
      a.refund_procedure_code,
      a.refund_gl_acct_num,
      ROUND(a.refund_payor_dw_id, 0, 'ROUND_HALF_EVEN') AS refund_payor_dw_id,
      ROUND(a.refund_payor_address_dw_id, 0, 'ROUND_HALF_EVEN') AS refund_payor_address_dw_id,
      a.refund_creation_date_time,
      a.refund_creation_user_id,
      ROUND(a.refund_amt, 3, 'ROUND_HALF_EVEN') AS refund_amt,
      ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(a.total_cash_pay_amt, 3, 'ROUND_HALF_EVEN') AS total_cash_pay_amt,
      ROUND(a.total_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_allow_amt,
      ROUND(a.total_policy_adj_amt, 3, 'ROUND_HALF_EVEN') AS total_policy_adj_amt,
      ROUND(a.total_write_off_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_amt,
      a.credit_balance_date,
      ROUND(a.current_grv_amt, 3, 'ROUND_HALF_EVEN') AS current_grv_amt,
      a.entered_date,
      a.discharge_date,
      a.admission_date,
      a.final_bill_date,
      a.bill_through_date,
      a.credit_bal_estb_date_time,
      a.last_update_date_time,
      a.last_update_user_id,
      a.approved_by_user_id,
      a.approved_date_time,
      a.auto_refund_create_date_time,
      a.resolved_date,
      a.eligible_transfer_ind,
      a.late_charge_ind,
      a.restore_date_time,
      a.restore_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_compliance_credit_inventory AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
