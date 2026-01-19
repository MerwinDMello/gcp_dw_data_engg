-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/gl_balance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.gl_balance AS SELECT
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
    ROUND(gl_balance.balance_amt, 3, 'ROUND_HALF_EVEN') AS balance_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.gl_balance
;
