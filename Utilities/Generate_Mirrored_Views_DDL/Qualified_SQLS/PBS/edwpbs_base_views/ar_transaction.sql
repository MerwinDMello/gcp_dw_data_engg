-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ar_transaction.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ar_transaction AS SELECT
    ar_transaction.company_code,
    ar_transaction.coid,
    ROUND(ar_transaction.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ar_transaction.ar_transaction_effective_date,
    ar_transaction.ar_transaction_enter_date,
    ar_transaction.ar_transaction_enter_time,
    ar_transaction.iplan_id,
    ROUND(ar_transaction.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    ar_transaction.source_system_code,
    ar_transaction.eff_from_date,
    ar_transaction.bad_debt_reason_code,
    ROUND(ar_transaction.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    ar_transaction.credit_gl_sub_account_num,
    ar_transaction.system_generated_ind,
    ROUND(ar_transaction.ar_transaction_amt, 3, 'ROUND_HALF_EVEN') AS ar_transaction_amt,
    ar_transaction.ar_transaction_source_code,
    ar_transaction.ar_transaction_batch_code,
    ar_transaction.ar_transaction_comment_text,
    ar_transaction.debit_cc_authorization_num,
    ar_transaction.check_num,
    ar_transaction.check_cleared_date,
    ar_transaction.adjustment_reason_code,
    ar_transaction.adjustment_gl_account_num,
    ROUND(ar_transaction.bad_debt_approval_id, 0, 'ROUND_HALF_EVEN') AS bad_debt_approval_id,
    ar_transaction.bad_debt_approval_date,
    ar_transaction.debit_gl_dept_num,
    ar_transaction.debit_gl_sub_account_num,
    ar_transaction.credit_gl_dept_num,
    ar_transaction.contractual_adj_reason_code,
    ROUND(ar_transaction.transaction_code, 0, 'ROUND_HALF_EVEN') AS transaction_code,
    ar_transaction.transaction_type_code,
    ar_transaction.icn,
    ar_transaction.pe_date,
    ar_transaction.hps_order_id,
    ar_transaction.admission_date,
    ar_transaction.discharge_date
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.ar_transaction
;
