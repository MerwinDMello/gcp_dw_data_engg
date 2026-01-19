-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/rpt_ar_ada_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.rpt_ar_ada_detail AS SELECT
    rpt_ar_ada_detail.company_code,
    rpt_ar_ada_detail.month_id,
    rpt_ar_ada_detail.patient_type_member_code,
    rpt_ar_ada_detail.coid,
    rpt_ar_ada_detail.journal_entry_ind,
    rpt_ar_ada_detail.unit_num,
    rpt_ar_ada_detail.secn_agcy_acct_bal_amt,
    rpt_ar_ada_detail.self_pay_ar_amt,
    rpt_ar_ada_detail.non_secn_self_pay_ar_amt,
    rpt_ar_ada_detail.non_secn_unins_disc_amt,
    rpt_ar_ada_detail.gross_non_secn_self_pay_ar_amt,
    rpt_ar_ada_detail.ada_accrual_end_bal_amt,
    rpt_ar_ada_detail.charity_accrual_end_bal_amt,
    rpt_ar_ada_detail.unins_accrual_end_bal_amt,
    rpt_ar_ada_detail.spca_accrual_end_bal_amt,
    rpt_ar_ada_detail.bad_debt_writeoff_amt,
    rpt_ar_ada_detail.source_system_code,
    rpt_ar_ada_detail.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.rpt_ar_ada_detail
;
