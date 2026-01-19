-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payment_compliance_credit_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_credit_inventory AS SELECT
    a.patient_dw_id,
    a.reporting_date,
    a.credit_balance_refund_ind,
    a.credit_balance_refund_id,
    a.company_code,
    a.coid,
    a.payor_dw_id_ins1,
    a.unit_num,
    a.pat_acct_num,
    a.admission_type_code,
    a.account_status_code,
    a.refund_type_sid,
    a.credit_status_sid,
    a.payor_financial_class_code,
    a.iplan_id_ins1,
    a.iplan_id_ins2,
    a.iplan_id_ins3,
    a.patient_person_dw_id,
    a.patient_address_dw_id,
    a.member_id,
    a.logged_ind,
    a.refund_iplan_id,
    a.refund_special_code,
    a.refund_procedure_code,
    a.refund_gl_acct_num,
    a.refund_payor_dw_id,
    a.refund_payor_address_dw_id,
    a.refund_creation_date_time,
    a.refund_creation_user_id,
    a.refund_amt,
    a.total_charge_amt,
    a.total_account_balance_amt,
    a.total_cash_pay_amt,
    a.total_allow_amt,
    a.total_policy_adj_amt,
    a.total_write_off_amt,
    a.credit_balance_date,
    a.current_grv_amt,
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
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
