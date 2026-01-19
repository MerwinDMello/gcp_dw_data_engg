-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_patient_finc_class_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient_finc_class_eom AS SELECT
    a.patient_dw_id,
    a.admission_month,
    a.coid,
    a.company_code,
    a.unit_num,
    a.pat_acct_num,
    a.derived_patient_type_code,
    a.gender_code,
    a.patient_admit_age_code,
    a.financial_class_code_init,
    a.financial_class_grp_code_init,
    a.iplan_id_ins1,
    a.iplan_id_ins2,
    a.iplan_id_ins3,
    a.totl_billed_chg_adm_eom_amt,
    a.financial_class_code_3mth,
    a.financial_class_grp_code_3mth,
    a.iplan_id_ins1_3mth,
    a.totl_payment_amt_3mth,
    a.financial_class_code_6mth,
    a.financial_class_grp_code_6mth,
    a.iplan_id_ins1_6mth,
    a.totl_payment_amt_6mth,
    a.financial_class_code_9mth,
    a.financial_class_grp_code_9mth,
    a.iplan_id_ins1_9mth,
    a.totl_payment_amt_9mth,
    a.financial_class_code_12mth,
    a.financial_class_grp_code_12mth,
    a.iplan_id_ins1_12mth,
    a.totl_payment_amt_12mth,
    a.totl_account_balance_amt_init,
    a.totl_account_balance_amt_3mth,
    a.totl_account_balance_amt_6mth,
    a.totl_account_balance_amt_9mth,
    a.totl_account_balance_amt_12mth,
    a.newborn_patient_ind
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_patient_finc_class_eom AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
