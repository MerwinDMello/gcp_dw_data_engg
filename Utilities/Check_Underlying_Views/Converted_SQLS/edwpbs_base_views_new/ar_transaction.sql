-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ar_transaction.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ar_transaction AS SELECT
    ar_transaction.company_code,
    ar_transaction.coid,
    ar_transaction.patient_dw_id,
    ar_transaction.ar_transaction_effective_date,
    ar_transaction.ar_transaction_enter_date,
    ar_transaction.ar_transaction_enter_time,
    ar_transaction.iplan_id,
    ar_transaction.pat_acct_num,
    ar_transaction.source_system_code,
    ar_transaction.eff_from_date,
    ar_transaction.bad_debt_reason_code,
    ar_transaction.payor_dw_id,
    ar_transaction.credit_gl_sub_account_num,
    ar_transaction.system_generated_ind,
    ar_transaction.ar_transaction_amt,
    ar_transaction.ar_transaction_source_code,
    ar_transaction.ar_transaction_batch_code,
    ar_transaction.ar_transaction_comment_text,
    ar_transaction.debit_cc_authorization_num,
    ar_transaction.check_num,
    ar_transaction.check_cleared_date,
    ar_transaction.adjustment_reason_code,
    ar_transaction.adjustment_gl_account_num,
    ar_transaction.bad_debt_approval_id,
    ar_transaction.bad_debt_approval_date,
    ar_transaction.debit_gl_dept_num,
    ar_transaction.debit_gl_sub_account_num,
    ar_transaction.credit_gl_dept_num,
    ar_transaction.contractual_adj_reason_code,
    ar_transaction.transaction_code,
    ar_transaction.transaction_type_code,
    ar_transaction.icn,
    ar_transaction.pe_date,
    ar_transaction.hps_order_id,
    ar_transaction.admission_date,
    ar_transaction.discharge_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.ar_transaction
;
