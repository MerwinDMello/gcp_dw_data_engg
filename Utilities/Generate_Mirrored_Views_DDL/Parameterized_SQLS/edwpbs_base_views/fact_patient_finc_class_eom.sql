-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_patient_finc_class_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.fact_patient_finc_class_eom
   OPTIONS(description='This table contains Patient Financial Class conversion for the current Month End cycle.')
  AS SELECT
      ROUND(fact_patient_finc_class_eom.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      fact_patient_finc_class_eom.admission_month,
      fact_patient_finc_class_eom.coid,
      fact_patient_finc_class_eom.company_code,
      fact_patient_finc_class_eom.unit_num,
      ROUND(fact_patient_finc_class_eom.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      fact_patient_finc_class_eom.derived_patient_type_code,
      fact_patient_finc_class_eom.gender_code,
      fact_patient_finc_class_eom.patient_admit_age_code,
      ROUND(fact_patient_finc_class_eom.financial_class_code_init, 0, 'ROUND_HALF_EVEN') AS financial_class_code_init,
      fact_patient_finc_class_eom.financial_class_grp_code_init,
      fact_patient_finc_class_eom.iplan_id_ins1,
      fact_patient_finc_class_eom.iplan_id_ins2,
      fact_patient_finc_class_eom.iplan_id_ins3,
      ROUND(fact_patient_finc_class_eom.totl_billed_chg_adm_eom_amt, 3, 'ROUND_HALF_EVEN') AS totl_billed_chg_adm_eom_amt,
      ROUND(fact_patient_finc_class_eom.financial_class_code_3mth, 0, 'ROUND_HALF_EVEN') AS financial_class_code_3mth,
      fact_patient_finc_class_eom.financial_class_grp_code_3mth,
      fact_patient_finc_class_eom.iplan_id_ins1_3mth,
      ROUND(fact_patient_finc_class_eom.totl_payment_amt_3mth, 3, 'ROUND_HALF_EVEN') AS totl_payment_amt_3mth,
      ROUND(fact_patient_finc_class_eom.financial_class_code_6mth, 0, 'ROUND_HALF_EVEN') AS financial_class_code_6mth,
      fact_patient_finc_class_eom.financial_class_grp_code_6mth,
      fact_patient_finc_class_eom.iplan_id_ins1_6mth,
      ROUND(fact_patient_finc_class_eom.totl_payment_amt_6mth, 3, 'ROUND_HALF_EVEN') AS totl_payment_amt_6mth,
      ROUND(fact_patient_finc_class_eom.financial_class_code_9mth, 0, 'ROUND_HALF_EVEN') AS financial_class_code_9mth,
      fact_patient_finc_class_eom.financial_class_grp_code_9mth,
      fact_patient_finc_class_eom.iplan_id_ins1_9mth,
      ROUND(fact_patient_finc_class_eom.totl_payment_amt_9mth, 3, 'ROUND_HALF_EVEN') AS totl_payment_amt_9mth,
      ROUND(fact_patient_finc_class_eom.financial_class_code_12mth, 0, 'ROUND_HALF_EVEN') AS financial_class_code_12mth,
      fact_patient_finc_class_eom.financial_class_grp_code_12mth,
      fact_patient_finc_class_eom.iplan_id_ins1_12mth,
      ROUND(fact_patient_finc_class_eom.totl_payment_amt_12mth, 3, 'ROUND_HALF_EVEN') AS totl_payment_amt_12mth,
      ROUND(fact_patient_finc_class_eom.totl_account_balance_amt_init, 3, 'ROUND_HALF_EVEN') AS totl_account_balance_amt_init,
      ROUND(fact_patient_finc_class_eom.totl_account_balance_amt_3mth, 3, 'ROUND_HALF_EVEN') AS totl_account_balance_amt_3mth,
      ROUND(fact_patient_finc_class_eom.totl_account_balance_amt_6mth, 3, 'ROUND_HALF_EVEN') AS totl_account_balance_amt_6mth,
      ROUND(fact_patient_finc_class_eom.totl_account_balance_amt_9mth, 3, 'ROUND_HALF_EVEN') AS totl_account_balance_amt_9mth,
      ROUND(fact_patient_finc_class_eom.totl_account_balance_amt_12mth, 3, 'ROUND_HALF_EVEN') AS totl_account_balance_amt_12mth,
      fact_patient_finc_class_eom.newborn_patient_ind
    FROM
      {{ params.param_pbs_core_dataset_name }}.fact_patient_finc_class_eom
  ;
