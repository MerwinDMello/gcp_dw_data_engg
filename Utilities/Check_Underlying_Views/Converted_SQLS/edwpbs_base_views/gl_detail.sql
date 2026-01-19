-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/gl_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.gl_detail AS SELECT
    gl_detail.coid,
    gl_detail.unit_num,
    gl_detail.company_code,
    gl_detail.journal_code,
    gl_detail.client_facility_id,
    gl_detail.effective_date1,
    gl_detail.entry_date,
    gl_detail.gl_dept,
    gl_detail.entry_control_num,
    gl_detail.entry_desc,
    ROUND(gl_detail.entry_total_debit_amt, 3, 'ROUND_HALF_EVEN') AS entry_total_debit_amt,
    ROUND(gl_detail.entry_total_credit_amt, 3, 'ROUND_HALF_EVEN') AS entry_total_credit_amt,
    gl_detail.acct_num,
    gl_detail.acct_name,
    ROUND(gl_detail.amount, 3, 'ROUND_HALF_EVEN') AS amount,
    gl_detail.line_item_desc1,
    gl_detail.line_item_desc2,
    gl_detail.line_item_desc3,
    gl_detail.acct_type_code,
    gl_detail.journal_line_num,
    gl_detail.effective_date2,
    gl_detail.balance_code,
    gl_detail.created_by,
    gl_detail.journal_account,
    gl_detail.comp_code,
    gl_detail.entry_description,
    gl_detail.line_item_seq_num,
    gl_detail.ref1,
    gl_detail.ref2,
    gl_detail.ref3,
    ROUND(gl_detail.line_activity_amt, 3, 'ROUND_HALF_EVEN') AS line_activity_amt,
    gl_detail.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.gl_detail
;
