-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ar_transaction_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ar_transaction_pf AS SELECT
    bv.company_code,
    bv.coid,
    ROUND(bv.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    bv.ar_transaction_effective_date,
    bv.ar_transaction_enter_date,
    bv.ar_transaction_enter_time,
    bv.iplan_id,
    ROUND(bv.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    bv.source_system_code,
    bv.eff_from_date,
    bv.bad_debt_reason_code,
    ROUND(bv.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    bv.credit_gl_sub_account_num,
    bv.system_generated_ind,
    ROUND(bv.ar_transaction_amt, 3, 'ROUND_HALF_EVEN') AS ar_transaction_amt,
    bv.ar_transaction_source_code,
    bv.ar_transaction_batch_code,
    bv.ar_transaction_comment_text,
    bv.debit_cc_authorization_num,
    bv.check_num,
    bv.check_cleared_date,
    bv.adjustment_reason_code,
    bv.adjustment_gl_account_num,
    ROUND(bv.bad_debt_approval_id, 0, 'ROUND_HALF_EVEN') AS bad_debt_approval_id,
    bv.bad_debt_approval_date,
    bv.debit_gl_dept_num,
    bv.debit_gl_sub_account_num,
    bv.credit_gl_dept_num,
    bv.contractual_adj_reason_code,
    ROUND(bv.transaction_code, 0, 'ROUND_HALF_EVEN') AS transaction_code,
    bv.transaction_type_code,
    bv.icn,
    bv.pe_date,
    bv.hps_order_id
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ar_transaction_pf AS bv
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON upper(bv.coid) = upper(sf.co_id)
     AND sf.user_id = session_user()
;
