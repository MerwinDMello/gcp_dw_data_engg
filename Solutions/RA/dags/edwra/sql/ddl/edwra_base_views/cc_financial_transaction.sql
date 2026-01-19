-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_financial_transaction.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_financial_transaction AS SELECT
    cc_financial_transaction.company_code,
    cc_financial_transaction.coid,
    cc_financial_transaction.patient_dw_id,
    cc_financial_transaction.account_transaction_id,
    cc_financial_transaction.unit_num,
    cc_financial_transaction.pat_acct_num,
    cc_financial_transaction.payor_dw_id,
    cc_financial_transaction.iplan_insurance_order_num,
    cc_financial_transaction.iplan_id,
    cc_financial_transaction.transaction_type,
    cc_financial_transaction.transaction_enter_date_time,
    cc_financial_transaction.transaction_eff_date_time,
    cc_financial_transaction.transaction_code,
    cc_financial_transaction.transaction_amt,
    cc_financial_transaction.transaction_bill_thru_date,
    cc_financial_transaction.transaction_comment_text,
    cc_financial_transaction.icn_num,
    cc_financial_transaction.status_category_id,
    cc_financial_transaction.reason_id,
    cc_financial_transaction.financial_period_id,
    cc_financial_transaction.appeal_num,
    cc_financial_transaction.appeal_seq_num,
    cc_financial_transaction.reversal_ind,
    cc_financial_transaction.redistributed_ind,
    cc_financial_transaction.parent_account_transaction_id,
    cc_financial_transaction.transaction_create_user_id,
    cc_financial_transaction.transaction_create_date_time,
    cc_financial_transaction.transaction_update_user_id,
    cc_financial_transaction.transaction_update_date_time,
    cc_financial_transaction.dw_last_update_date_time,
    cc_financial_transaction.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.cc_financial_transaction
;
