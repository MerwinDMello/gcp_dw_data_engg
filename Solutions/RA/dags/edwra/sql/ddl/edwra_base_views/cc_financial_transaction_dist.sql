-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_financial_transaction_dist.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_financial_transaction_dist AS SELECT
    cc_financial_transaction_dist.company_code,
    cc_financial_transaction_dist.coid,
    cc_financial_transaction_dist.patient_dw_id,
    cc_financial_transaction_dist.account_transaction_id,
    cc_financial_transaction_dist.redistribution_category_id,
    cc_financial_transaction_dist.redistribution_date_time,
    cc_financial_transaction_dist.unit_num,
    cc_financial_transaction_dist.pat_acct_num,
    cc_financial_transaction_dist.redistribution_user_id,
    cc_financial_transaction_dist.redistribution_amt,
    cc_financial_transaction_dist.dw_last_update_date_time,
    cc_financial_transaction_dist.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.cc_financial_transaction_dist
;
