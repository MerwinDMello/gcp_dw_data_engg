-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/mcare_bad_debt_crossover_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.mcare_bad_debt_crossover_dtl
   OPTIONS(description='This table contains all the relevant information on the  accounts having Medicare responsibility.')
  AS SELECT
      a.reporting_date,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.company_code,
      a.coid,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.admission_date,
      a.discharge_date,
      a.final_bill_date,
      a.reason_desc,
      a.account_payer_status_desc,
      a.project_name,
      a.account_status_code,
      a.patient_type_code,
      a.iplan_id_ins1,
      a.iplan_id_ins2,
      a.iplan_id_ins3,
      a.plan_desc_ins1,
      ROUND(a.plan_desc_ins2, 0, 'ROUND_HALF_EVEN') AS plan_desc_ins2,
      a.plan_desc_ins3,
      ROUND(a.financial_class_code_ins1, 0, 'ROUND_HALF_EVEN') AS financial_class_code_ins1,
      ROUND(a.financial_class_code_ins2, 0, 'ROUND_HALF_EVEN') AS financial_class_code_ins2,
      ROUND(a.financial_class_code_ins3, 0, 'ROUND_HALF_EVEN') AS financial_class_code_ins3,
      a.bill_code_ins1,
      a.bill_code_ins2,
      a.bill_code_ins3,
      a.procedure_code,
      ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(a.total_expt_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_expt_payment_amt,
      ROUND(a.total_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_payment_amt,
      ROUND(a.total_variance_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS total_variance_adjustment_amt,
      ROUND(a.total_pat_responsibility_amt, 3, 'ROUND_HALF_EVEN') AS total_pat_responsibility_amt,
      ROUND(a.primary_mcare_responsibility_amt, 3, 'ROUND_HALF_EVEN') AS primary_mcare_responsibility_amt,
      ROUND(a.patient_payment_amt, 3, 'ROUND_HALF_EVEN') AS patient_payment_amt,
      ROUND(a.primary_payment_amt, 3, 'ROUND_HALF_EVEN') AS primary_payment_amt,
      ROUND(a.secondary_payment_amt, 3, 'ROUND_HALF_EVEN') AS secondary_payment_amt,
      a.latest_primary_payment_date,
      a.ep_payment_create_date,
      a.ep_payment_effective_post_date,
      a.era_create_date,
      a.check_date,
      a.service_carc_claim_carc_flag,
      a.service_carc_ind,
      a.service_carc_denial_cd_23_ind,
      a.service_carc_denial_cd_45_ind,
      a.service_carc_denial_cd_97_ind,
      a.calc_claim_carc_23_45_97_ind,
      a.claim_carc_ind,
      a.claim_carc_denial_cd_23_ind,
      a.claim_carc_denial_cd_45_ind,
      a.claim_carc_denial_cd_97_ind,
      a.addl_service_carc_ind,
      a.service_carc_denial_cd_22_ind,
      a.service_carc_denial_cd_26_ind,
      a.service_carc_denial_cd_27_ind,
      a.service_carc_denial_cd_29_ind,
      a.service_carc_denial_cd_31_ind,
      a.service_carc_denial_cd_109_ind,
      a.calc_addl_claim_carc_ind,
      a.addl_claim_carc_ind,
      a.claim_carc_denial_cd_22_ind,
      a.claim_carc_denial_cd_26_ind,
      a.claim_carc_denial_cd_27_ind,
      a.claim_carc_denial_cd_29_ind,
      a.claim_carc_denial_cd_31_ind,
      a.claim_carc_denial_cd_109_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.mcare_bad_debt_crossover_dtl AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
