-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ar_transaction_full.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ar_transaction_full AS SELECT
    bv.company_code,
    bv.coid,
    bv.patient_dw_id,
    bv.ar_transaction_effective_date,
    bv.ar_transaction_enter_date,
    bv.ar_transaction_enter_time,
    bv.iplan_id,
    bv.pat_acct_num,
    bv.source_system_code,
    bv.eff_from_date,
    bv.bad_debt_reason_code,
    bv.payor_dw_id,
    bv.credit_gl_sub_account_num,
    bv.system_generated_ind,
    bv.ar_transaction_amt,
    bv.ar_transaction_source_code,
    bv.ar_transaction_batch_code,
    bv.ar_transaction_comment_text,
    bv.debit_cc_authorization_num,
    bv.check_num,
    bv.check_cleared_date,
    bv.adjustment_reason_code,
    bv.adjustment_gl_account_num,
    bv.bad_debt_approval_id,
    bv.bad_debt_approval_date,
    bv.debit_gl_dept_num,
    bv.debit_gl_sub_account_num,
    bv.credit_gl_dept_num,
    bv.contractual_adj_reason_code,
    bv.transaction_code,
    bv.transaction_type_code,
    bv.icn,
    bv.pe_date,
    bv.hps_order_id,
    'PBS' AS source_db_name
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ar_transaction AS bv
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON bv.coid = sf.co_id
     AND sf.user_id = session_user()
  WHERE bv.source_system_code = 'P'
UNION ALL
SELECT
    pf.company_code,
    pf.coid,
    pf.patient_dw_id,
    pf.ar_transaction_effective_date,
    pf.ar_transaction_enter_date,
    pf.ar_transaction_enter_time,
    pf.iplan_id,
    pf.pat_acct_num,
    pf.source_system_code,
    pf.eff_from_date,
    pf.bad_debt_reason_code,
    pf.payor_dw_id,
    pf.credit_gl_sub_account_num,
    pf.system_generated_ind,
    pf.ar_transaction_amt,
    pf.ar_transaction_source_code,
    pf.ar_transaction_batch_code,
    pf.ar_transaction_comment_text,
    pf.debit_cc_authorization_num,
    pf.check_num,
    pf.check_cleared_date,
    pf.adjustment_reason_code,
    pf.adjustment_gl_account_num,
    pf.bad_debt_approval_id,
    pf.bad_debt_approval_date,
    pf.debit_gl_dept_num,
    pf.debit_gl_sub_account_num,
    pf.credit_gl_dept_num,
    pf.contractual_adj_reason_code,
    pf.transaction_code,
    pf.transaction_type_code,
    pf.icn,
    pf.pe_date,
    pf.hps_order_id,
    'PF' AS source_db_name
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ar_transaction_pf AS pf
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON pf.coid = sf.co_id
     AND sf.user_id = session_user()
  WHERE pf.source_system_code = 'P'
;
