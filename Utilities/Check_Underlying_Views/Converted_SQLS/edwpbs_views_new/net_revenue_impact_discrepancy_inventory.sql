-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/net_revenue_impact_discrepancy_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.net_revenue_impact_discrepancy_inventory AS SELECT
    a.reporting_date,
    a.patient_dw_id,
    a.eor_log_date,
    a.log_id,
    a.log_sequence_num,
    a.company_code,
    a.coid,
    a.payor_dw_id,
    a.iplan_insurance_order_num,
    a.eff_from_date,
    a.pat_acct_num,
    a.iplan_id,
    a.remittance_date,
    a.discrepancy_origination_date,
    a.reason_assignment_date_1,
    a.reason_assignment_date_2,
    a.reason_assignment_date_3,
    a.reason_assignment_date_4,
    a.over_under_payment_amt,
    a.actual_payment_amt,
    a.var_total_charge_amt,
    a.var_gross_reimbursement_amt,
    a.var_primary_payor_pay_amt,
    a.total_account_balance_amt,
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
    a.cc_calc_id,
    a.cc_account_activity_id,
    a.cc_reason_id,
    a.cc_account_payer_status_id,
    a.admission_date,
    a.discharge_date,
    a.financial_class_code,
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
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.net_revenue_impact_discrepancy_inventory AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
