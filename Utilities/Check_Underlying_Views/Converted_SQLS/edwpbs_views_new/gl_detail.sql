-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
    a.entry_total_debit_amt,
    a.entry_total_credit_amt,
    a.acct_num,
    a.acct_name,
    a.amount,
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
    a.line_activity_amt,
    a.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.gl_detail AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.secref_facility AS sf ON a.coid = sf.co_id
     AND a.company_code = sf.company_code
     AND session_user() = sf.user_id
;
