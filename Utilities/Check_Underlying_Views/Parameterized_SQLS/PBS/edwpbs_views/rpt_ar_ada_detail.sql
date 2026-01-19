-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/rpt_ar_ada_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.rpt_ar_ada_detail
   OPTIONS(description='Reporting detail for Allowance for Doubtful account reports')
  AS SELECT
      a.company_code,
      a.month_id,
      a.patient_type_member_code,
      a.coid,
      a.journal_entry_ind,
      a.unit_num,
      ROUND(a.secn_agcy_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS secn_agcy_acct_bal_amt,
      ROUND(a.self_pay_ar_amt, 3, 'ROUND_HALF_EVEN') AS self_pay_ar_amt,
      ROUND(a.non_secn_self_pay_ar_amt, 3, 'ROUND_HALF_EVEN') AS non_secn_self_pay_ar_amt,
      ROUND(a.non_secn_unins_disc_amt, 3, 'ROUND_HALF_EVEN') AS non_secn_unins_disc_amt,
      ROUND(a.gross_non_secn_self_pay_ar_amt, 3, 'ROUND_HALF_EVEN') AS gross_non_secn_self_pay_ar_amt,
      ROUND(a.ada_accrual_end_bal_amt, 3, 'ROUND_HALF_EVEN') AS ada_accrual_end_bal_amt,
      ROUND(a.charity_accrual_end_bal_amt, 3, 'ROUND_HALF_EVEN') AS charity_accrual_end_bal_amt,
      ROUND(a.unins_accrual_end_bal_amt, 3, 'ROUND_HALF_EVEN') AS unins_accrual_end_bal_amt,
      ROUND(a.spca_accrual_end_bal_amt, 3, 'ROUND_HALF_EVEN') AS spca_accrual_end_bal_amt,
      ROUND(a.bad_debt_writeoff_amt, 3, 'ROUND_HALF_EVEN') AS bad_debt_writeoff_amt,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.rpt_ar_ada_detail AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
