-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/rpt_ar_ada_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.rpt_ar_ada_detail AS SELECT
    a.company_code,
    a.month_id,
    a.patient_type_member_code,
    a.coid,
    a.journal_entry_ind,
    a.unit_num,
    a.secn_agcy_acct_bal_amt,
    a.self_pay_ar_amt,
    a.non_secn_self_pay_ar_amt,
    a.non_secn_unins_disc_amt,
    a.gross_non_secn_self_pay_ar_amt,
    a.ada_accrual_end_bal_amt,
    a.charity_accrual_end_bal_amt,
    a.unins_accrual_end_bal_amt,
    a.spca_accrual_end_bal_amt,
    a.bad_debt_writeoff_amt,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.rpt_ar_ada_detail AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
