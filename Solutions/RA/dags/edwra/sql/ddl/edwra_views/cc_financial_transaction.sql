-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_financial_transaction.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_financial_transaction AS SELECT
    a.company_code,
    a.coid,
    a.patient_dw_id,
    a.account_transaction_id,
    a.unit_num,
    a.pat_acct_num,
    a.payor_dw_id,
    a.iplan_insurance_order_num,
    a.iplan_id,
    a.transaction_type,
    a.transaction_enter_date_time,
    a.transaction_eff_date_time,
    a.transaction_code,
    a.transaction_amt,
    a.transaction_bill_thru_date,
    a.transaction_comment_text,
    a.icn_num,
    a.status_category_id,
    a.reason_id,
    a.financial_period_id,
    a.appeal_num,
    a.appeal_seq_num,
    a.reversal_ind,
    a.redistributed_ind,
    a.parent_account_transaction_id,
    a.transaction_create_user_id,
    a.transaction_create_date_time,
    a.transaction_update_user_id,
    a.transaction_update_date_time,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.cc_financial_transaction AS a
    INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
     AND rtrim(a.coid) = rtrim(b.co_id)
     AND rtrim(b.user_id) = rtrim(session_user())
;
