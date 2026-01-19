-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/gl_balance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.gl_balance AS SELECT
    gl_balance.coid,
    gl_balance.unit_num,
    gl_balance.company_code,
    gl_balance.client_facility_id,
    gl_balance.effective_period,
    gl_balance.effective_year,
    gl_balance.gl_dept,
    gl_balance.acct_type_code,
    gl_balance.acct_num,
    gl_balance.acct_name,
    gl_balance.debit_credit_ind,
    gl_balance.balance_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.gl_balance
;
