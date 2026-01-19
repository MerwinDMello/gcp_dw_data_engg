-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/gl_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.gl_detail AS SELECT
    a.coid,
    a.unit_num,
    a.company_code,
    a.journal_code,
    a.client_facility_id,
    a.effective_date1,
    a.entry_date,
    a.gl_dept,
    a.entry_control_num,
    a.entry_desc,
    ROUND(a.entry_total_debit_amt, 3, 'ROUND_HALF_EVEN') AS entry_total_debit_amt,
    ROUND(a.entry_total_credit_amt, 3, 'ROUND_HALF_EVEN') AS entry_total_credit_amt,
    a.acct_num,
    a.acct_name,
    ROUND(a.amount, 3, 'ROUND_HALF_EVEN') AS amount,
    a.line_item_desc1,
    a.line_item_desc2,
    a.line_item_desc3,
    a.acct_type_code,
    a.journal_line_num,
    a.effective_date2,
    a.balance_code,
    a.created_by,
    a.journal_account,
    a.comp_code,
    a.entry_description,
    a.line_item_seq_num,
    a.ref1,
    a.ref2,
    a.ref3,
    ROUND(a.line_activity_amt, 3, 'ROUND_HALF_EVEN') AS line_activity_amt,
    a.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.gl_detail AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.secref_facility AS sf ON upper(a.coid) = upper(sf.co_id)
     AND upper(a.company_code) = upper(sf.company_code)
     AND session_user() = sf.user_id
;
