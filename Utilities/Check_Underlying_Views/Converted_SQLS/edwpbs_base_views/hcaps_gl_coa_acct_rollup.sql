-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/hcaps_gl_coa_acct_rollup.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.hcaps_gl_coa_acct_rollup AS SELECT
    hcaps_gl_coa_acct_rollup.coid,
    hcaps_gl_coa_acct_rollup.company_code,
    hcaps_gl_coa_acct_rollup.gl_dept_num,
    hcaps_gl_coa_acct_rollup.gl_sub_account_num,
    hcaps_gl_coa_acct_rollup.hcaps_gl_account_desc,
    hcaps_gl_coa_acct_rollup.hcaps_fscode,
    hcaps_gl_coa_acct_rollup.data_source_code,
    hcaps_gl_coa_acct_rollup.account_type_code,
    hcaps_gl_coa_acct_rollup.source_system_code,
    hcaps_gl_coa_acct_rollup.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwps_base_views.hcaps_gl_coa_acct_rollup
;
