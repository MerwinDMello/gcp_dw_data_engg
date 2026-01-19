-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
    gl_detail.entry_total_debit_amt,
    gl_detail.entry_total_credit_amt,
    gl_detail.acct_num,
    gl_detail.acct_name,
    gl_detail.amount,
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
    gl_detail.line_activity_amt,
    gl_detail.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.gl_detail
;
