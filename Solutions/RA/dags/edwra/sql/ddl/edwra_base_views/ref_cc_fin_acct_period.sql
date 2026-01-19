-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_fin_acct_period.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_fin_acct_period AS SELECT
    ref_cc_fin_acct_period.company_code,
    ref_cc_fin_acct_period.coid,
    ref_cc_fin_acct_period.financial_period_id,
    ref_cc_fin_acct_period.fiscal_year,
    ref_cc_fin_acct_period.acctg_period_num,
    ref_cc_fin_acct_period.period_start_date,
    ref_cc_fin_acct_period.period_end_date,
    ref_cc_fin_acct_period.prelim_close_date,
    ref_cc_fin_acct_period.update_login_userid,
    ref_cc_fin_acct_period.update_date_time,
    ref_cc_fin_acct_period.dw_last_update_date_time,
    ref_cc_fin_acct_period.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_fin_acct_period
;
