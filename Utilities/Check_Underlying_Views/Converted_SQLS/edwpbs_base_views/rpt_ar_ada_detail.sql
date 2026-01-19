-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/rpt_ar_ada_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.rpt_ar_ada_detail
   OPTIONS(description='Reporting detail for Allowance for Doubtful account reports')
  AS SELECT
      rpt_ar_ada_detail.company_code,
      rpt_ar_ada_detail.month_id,
      rpt_ar_ada_detail.patient_type_member_code,
      rpt_ar_ada_detail.coid,
      rpt_ar_ada_detail.journal_entry_ind,
      rpt_ar_ada_detail.unit_num,
      ROUND(rpt_ar_ada_detail.secn_agcy_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS secn_agcy_acct_bal_amt,
      ROUND(rpt_ar_ada_detail.self_pay_ar_amt, 3, 'ROUND_HALF_EVEN') AS self_pay_ar_amt,
      ROUND(rpt_ar_ada_detail.non_secn_self_pay_ar_amt, 3, 'ROUND_HALF_EVEN') AS non_secn_self_pay_ar_amt,
      ROUND(rpt_ar_ada_detail.non_secn_unins_disc_amt, 3, 'ROUND_HALF_EVEN') AS non_secn_unins_disc_amt,
      ROUND(rpt_ar_ada_detail.gross_non_secn_self_pay_ar_amt, 3, 'ROUND_HALF_EVEN') AS gross_non_secn_self_pay_ar_amt,
      ROUND(rpt_ar_ada_detail.ada_accrual_end_bal_amt, 3, 'ROUND_HALF_EVEN') AS ada_accrual_end_bal_amt,
      ROUND(rpt_ar_ada_detail.charity_accrual_end_bal_amt, 3, 'ROUND_HALF_EVEN') AS charity_accrual_end_bal_amt,
      ROUND(rpt_ar_ada_detail.unins_accrual_end_bal_amt, 3, 'ROUND_HALF_EVEN') AS unins_accrual_end_bal_amt,
      ROUND(rpt_ar_ada_detail.spca_accrual_end_bal_amt, 3, 'ROUND_HALF_EVEN') AS spca_accrual_end_bal_amt,
      ROUND(rpt_ar_ada_detail.bad_debt_writeoff_amt, 3, 'ROUND_HALF_EVEN') AS bad_debt_writeoff_amt,
      rpt_ar_ada_detail.source_system_code,
      rpt_ar_ada_detail.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.rpt_ar_ada_detail
  ;
