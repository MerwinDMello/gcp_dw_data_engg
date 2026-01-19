-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_patient_finc_class_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_patient_finc_class_eom AS SELECT
    fact_patient_finc_class_eom.patient_dw_id,
    fact_patient_finc_class_eom.admission_month,
    fact_patient_finc_class_eom.coid,
    fact_patient_finc_class_eom.company_code,
    fact_patient_finc_class_eom.unit_num,
    fact_patient_finc_class_eom.pat_acct_num,
    fact_patient_finc_class_eom.derived_patient_type_code,
    fact_patient_finc_class_eom.gender_code,
    fact_patient_finc_class_eom.patient_admit_age_code,
    fact_patient_finc_class_eom.financial_class_code_init,
    fact_patient_finc_class_eom.financial_class_grp_code_init,
    fact_patient_finc_class_eom.iplan_id_ins1,
    fact_patient_finc_class_eom.iplan_id_ins2,
    fact_patient_finc_class_eom.iplan_id_ins3,
    fact_patient_finc_class_eom.totl_billed_chg_adm_eom_amt,
    fact_patient_finc_class_eom.financial_class_code_3mth,
    fact_patient_finc_class_eom.financial_class_grp_code_3mth,
    fact_patient_finc_class_eom.iplan_id_ins1_3mth,
    fact_patient_finc_class_eom.totl_payment_amt_3mth,
    fact_patient_finc_class_eom.financial_class_code_6mth,
    fact_patient_finc_class_eom.financial_class_grp_code_6mth,
    fact_patient_finc_class_eom.iplan_id_ins1_6mth,
    fact_patient_finc_class_eom.totl_payment_amt_6mth,
    fact_patient_finc_class_eom.financial_class_code_9mth,
    fact_patient_finc_class_eom.financial_class_grp_code_9mth,
    fact_patient_finc_class_eom.iplan_id_ins1_9mth,
    fact_patient_finc_class_eom.totl_payment_amt_9mth,
    fact_patient_finc_class_eom.financial_class_code_12mth,
    fact_patient_finc_class_eom.financial_class_grp_code_12mth,
    fact_patient_finc_class_eom.iplan_id_ins1_12mth,
    fact_patient_finc_class_eom.totl_payment_amt_12mth,
    fact_patient_finc_class_eom.totl_account_balance_amt_init,
    fact_patient_finc_class_eom.totl_account_balance_amt_3mth,
    fact_patient_finc_class_eom.totl_account_balance_amt_6mth,
    fact_patient_finc_class_eom.totl_account_balance_amt_9mth,
    fact_patient_finc_class_eom.totl_account_balance_amt_12mth,
    fact_patient_finc_class_eom.newborn_patient_ind
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.fact_patient_finc_class_eom
;
