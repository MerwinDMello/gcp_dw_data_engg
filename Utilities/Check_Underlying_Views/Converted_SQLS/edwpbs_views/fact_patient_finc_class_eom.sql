-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_patient_finc_class_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient_finc_class_eom
   OPTIONS(description='This table contains Patient Financial Class conversion for the current Month End cycle.')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.admission_month,
      a.coid,
      a.company_code,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.derived_patient_type_code,
      a.gender_code,
      a.patient_admit_age_code,
      ROUND(a.financial_class_code_init, 0, 'ROUND_HALF_EVEN') AS financial_class_code_init,
      a.financial_class_grp_code_init,
      a.iplan_id_ins1,
      a.iplan_id_ins2,
      a.iplan_id_ins3,
      ROUND(a.totl_billed_chg_adm_eom_amt, 3, 'ROUND_HALF_EVEN') AS totl_billed_chg_adm_eom_amt,
      ROUND(a.financial_class_code_3mth, 0, 'ROUND_HALF_EVEN') AS financial_class_code_3mth,
      a.financial_class_grp_code_3mth,
      a.iplan_id_ins1_3mth,
      ROUND(a.totl_payment_amt_3mth, 3, 'ROUND_HALF_EVEN') AS totl_payment_amt_3mth,
      ROUND(a.financial_class_code_6mth, 0, 'ROUND_HALF_EVEN') AS financial_class_code_6mth,
      a.financial_class_grp_code_6mth,
      a.iplan_id_ins1_6mth,
      ROUND(a.totl_payment_amt_6mth, 3, 'ROUND_HALF_EVEN') AS totl_payment_amt_6mth,
      ROUND(a.financial_class_code_9mth, 0, 'ROUND_HALF_EVEN') AS financial_class_code_9mth,
      a.financial_class_grp_code_9mth,
      a.iplan_id_ins1_9mth,
      ROUND(a.totl_payment_amt_9mth, 3, 'ROUND_HALF_EVEN') AS totl_payment_amt_9mth,
      ROUND(a.financial_class_code_12mth, 0, 'ROUND_HALF_EVEN') AS financial_class_code_12mth,
      a.financial_class_grp_code_12mth,
      a.iplan_id_ins1_12mth,
      ROUND(a.totl_payment_amt_12mth, 3, 'ROUND_HALF_EVEN') AS totl_payment_amt_12mth,
      ROUND(a.totl_account_balance_amt_init, 3, 'ROUND_HALF_EVEN') AS totl_account_balance_amt_init,
      ROUND(a.totl_account_balance_amt_3mth, 3, 'ROUND_HALF_EVEN') AS totl_account_balance_amt_3mth,
      ROUND(a.totl_account_balance_amt_6mth, 3, 'ROUND_HALF_EVEN') AS totl_account_balance_amt_6mth,
      ROUND(a.totl_account_balance_amt_9mth, 3, 'ROUND_HALF_EVEN') AS totl_account_balance_amt_9mth,
      ROUND(a.totl_account_balance_amt_12mth, 3, 'ROUND_HALF_EVEN') AS totl_account_balance_amt_12mth,
      a.newborn_patient_ind
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_patient_finc_class_eom AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
