-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
