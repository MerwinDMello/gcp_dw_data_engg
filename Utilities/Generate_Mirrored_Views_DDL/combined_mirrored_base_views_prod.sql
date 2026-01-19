-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ada_patient_level_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ada_patient_level_detail
   OPTIONS(description='Account level detail for Allowance for Doubtful accounts')
  AS SELECT
      ROUND(ada_patient_level_detail.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      ada_patient_level_detail.month_id,
      ada_patient_level_detail.company_code,
      ada_patient_level_detail.coid,
      ROUND(ada_patient_level_detail.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ada_patient_level_detail.unit_num,
      ada_patient_level_detail.account_status_code,
      ada_patient_level_detail.patient_type_code,
      ada_patient_level_detail.derived_patient_type_code,
      ROUND(ada_patient_level_detail.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      ROUND(ada_patient_level_detail.payor_dw_id_ins1, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins1,
      ROUND(ada_patient_level_detail.payor_dw_id_ins2, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins2,
      ROUND(ada_patient_level_detail.payor_dw_id_ins3, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins3,
      ada_patient_level_detail.agency_code,
      ada_patient_level_detail.agency_type_code,
      ada_patient_level_detail.admission_date,
      ada_patient_level_detail.discharge_date,
      ada_patient_level_detail.final_bill_date,
      ROUND(ada_patient_level_detail.inhouse_charity_amt, 3, 'ROUND_HALF_EVEN') AS inhouse_charity_amt,
      ROUND(ada_patient_level_detail.insured_charity_amt, 3, 'ROUND_HALF_EVEN') AS insured_charity_amt,
      ROUND(ada_patient_level_detail.insured_self_pay_amt, 3, 'ROUND_HALF_EVEN') AS insured_self_pay_amt,
      ROUND(ada_patient_level_detail.uninsured_discount_amt, 3, 'ROUND_HALF_EVEN') AS uninsured_discount_amt,
      ROUND(ada_patient_level_detail.charity_discount_amt, 3, 'ROUND_HALF_EVEN') AS charity_discount_amt,
      ROUND(ada_patient_level_detail.patient_liab_prot_discount_amt, 3, 'ROUND_HALF_EVEN') AS patient_liab_prot_discount_amt,
      ROUND(ada_patient_level_detail.total_discount_amt, 3, 'ROUND_HALF_EVEN') AS total_discount_amt,
      ROUND(ada_patient_level_detail.secn_agcy_unins_discount_amt, 3, 'ROUND_HALF_EVEN') AS secn_agcy_unins_discount_amt,
      ROUND(ada_patient_level_detail.secn_agcy_charity_discount_amt, 3, 'ROUND_HALF_EVEN') AS secn_agcy_charity_discount_amt,
      ROUND(ada_patient_level_detail.secn_agcy_pat_liab_prot_discount_amt, 3, 'ROUND_HALF_EVEN') AS secn_agcy_pat_liab_prot_discount_amt,
      ROUND(ada_patient_level_detail.total_secn_agcy_discount_amt, 3, 'ROUND_HALF_EVEN') AS total_secn_agcy_discount_amt,
      ROUND(ada_patient_level_detail.non_secn_unins_disc_amt, 3, 'ROUND_HALF_EVEN') AS non_secn_unins_disc_amt,
      ROUND(ada_patient_level_detail.non_secn_charity_discount_amt, 3, 'ROUND_HALF_EVEN') AS non_secn_charity_discount_amt,
      ROUND(ada_patient_level_detail.non_secn_patient_liab_prot_discount_amt, 3, 'ROUND_HALF_EVEN') AS non_secn_patient_liab_prot_discount_amt,
      ROUND(ada_patient_level_detail.total_non_secn_discount_amt, 3, 'ROUND_HALF_EVEN') AS total_non_secn_discount_amt,
      ROUND(ada_patient_level_detail.self_pay_ar_amt, 3, 'ROUND_HALF_EVEN') AS self_pay_ar_amt,
      ROUND(ada_patient_level_detail.secn_agcy_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS secn_agcy_acct_bal_amt,
      ROUND(ada_patient_level_detail.non_secn_self_pay_ar_amt, 3, 'ROUND_HALF_EVEN') AS non_secn_self_pay_ar_amt,
      ROUND(ada_patient_level_detail.gross_non_secn_self_pay_ar_amt, 3, 'ROUND_HALF_EVEN') AS gross_non_secn_self_pay_ar_amt,
      ada_patient_level_detail.iplan_id_ins1,
      ROUND(ada_patient_level_detail.payor_balance_amt_ins1, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_ins1,
      ROUND(ada_patient_level_detail.payor_payment_amt_ins1, 3, 'ROUND_HALF_EVEN') AS payor_payment_amt_ins1,
      ROUND(ada_patient_level_detail.payor_adjustment_amt_ins1, 3, 'ROUND_HALF_EVEN') AS payor_adjustment_amt_ins1,
      ROUND(ada_patient_level_detail.payor_contractual_amt_ins1, 3, 'ROUND_HALF_EVEN') AS payor_contractual_amt_ins1,
      ada_patient_level_detail.iplan_id_ins2,
      ROUND(ada_patient_level_detail.payor_balance_amt_ins2, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_ins2,
      ROUND(ada_patient_level_detail.payor_payment_amt_ins2, 3, 'ROUND_HALF_EVEN') AS payor_payment_amt_ins2,
      ROUND(ada_patient_level_detail.payor_adjustment_amt_ins2, 3, 'ROUND_HALF_EVEN') AS payor_adjustment_amt_ins2,
      ROUND(ada_patient_level_detail.payor_contractual_amt_ins2, 3, 'ROUND_HALF_EVEN') AS payor_contractual_amt_ins2,
      ada_patient_level_detail.iplan_id_ins3,
      ROUND(ada_patient_level_detail.payor_balance_amt_ins3, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_ins3,
      ROUND(ada_patient_level_detail.payor_payment_amt_ins3, 3, 'ROUND_HALF_EVEN') AS payor_payment_amt_ins3,
      ROUND(ada_patient_level_detail.payor_adjustment_amt_ins3, 3, 'ROUND_HALF_EVEN') AS payor_adjustment_amt_ins3,
      ROUND(ada_patient_level_detail.payor_contractual_amt_ins3, 3, 'ROUND_HALF_EVEN') AS payor_contractual_amt_ins3,
      ROUND(ada_patient_level_detail.patient_balance_amt, 3, 'ROUND_HALF_EVEN') AS patient_balance_amt,
      ROUND(ada_patient_level_detail.patient_payment_amt, 3, 'ROUND_HALF_EVEN') AS patient_payment_amt,
      ROUND(ada_patient_level_detail.patient_adj_amt, 3, 'ROUND_HALF_EVEN') AS patient_adj_amt,
      ROUND(ada_patient_level_detail.patient_allowance_amt, 3, 'ROUND_HALF_EVEN') AS patient_allowance_amt,
      ROUND(ada_patient_level_detail.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
      ROUND(ada_patient_level_detail.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(ada_patient_level_detail.total_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_payment_amt,
      ROUND(ada_patient_level_detail.total_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS total_adjustment_amt,
      ROUND(ada_patient_level_detail.total_combined_adj_alw_amt, 3, 'ROUND_HALF_EVEN') AS total_combined_adj_alw_amt,
      ROUND(ada_patient_level_detail.total_contract_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_contract_allow_amt,
      ROUND(ada_patient_level_detail.total_patient_due_amt, 3, 'ROUND_HALF_EVEN') AS total_patient_due_amt,
      ROUND(ada_patient_level_detail.total_write_off_bad_debt_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_bad_debt_amt,
      ROUND(ada_patient_level_detail.total_write_off_other_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_other_amt,
      ROUND(ada_patient_level_detail.discharged_not_final_bill_self_pay_amt, 3, 'ROUND_HALF_EVEN') AS discharged_not_final_bill_self_pay_amt,
      ROUND(ada_patient_level_detail.inhouse_self_pay_amt, 3, 'ROUND_HALF_EVEN') AS inhouse_self_pay_amt,
      ROUND(ada_patient_level_detail.discharged_not_final_bill_charity_amt, 3, 'ROUND_HALF_EVEN') AS discharged_not_final_bill_charity_amt,
      ROUND(ada_patient_level_detail.financial_class_code_ins1, 0, 'ROUND_HALF_EVEN') AS financial_class_code_ins1,
      ROUND(ada_patient_level_detail.financial_class_code_ins2, 0, 'ROUND_HALF_EVEN') AS financial_class_code_ins2,
      ROUND(ada_patient_level_detail.financial_class_code_ins3, 0, 'ROUND_HALF_EVEN') AS financial_class_code_ins3,
      ada_patient_level_detail.source_system_code,
      ada_patient_level_detail.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ada_patient_level_detail
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ar_transaction.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ar_transaction AS SELECT
    ar_transaction.company_code,
    ar_transaction.coid,
    ROUND(ar_transaction.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ar_transaction.ar_transaction_effective_date,
    ar_transaction.ar_transaction_enter_date,
    ar_transaction.ar_transaction_enter_time,
    ar_transaction.iplan_id,
    ROUND(ar_transaction.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    ar_transaction.source_system_code,
    ar_transaction.eff_from_date,
    ar_transaction.bad_debt_reason_code,
    ROUND(ar_transaction.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    ar_transaction.credit_gl_sub_account_num,
    ar_transaction.system_generated_ind,
    ROUND(ar_transaction.ar_transaction_amt, 3, 'ROUND_HALF_EVEN') AS ar_transaction_amt,
    ar_transaction.ar_transaction_source_code,
    ar_transaction.ar_transaction_batch_code,
    ar_transaction.ar_transaction_comment_text,
    ar_transaction.debit_cc_authorization_num,
    ar_transaction.check_num,
    ar_transaction.check_cleared_date,
    ar_transaction.adjustment_reason_code,
    ar_transaction.adjustment_gl_account_num,
    ROUND(ar_transaction.bad_debt_approval_id, 0, 'ROUND_HALF_EVEN') AS bad_debt_approval_id,
    ar_transaction.bad_debt_approval_date,
    ar_transaction.debit_gl_dept_num,
    ar_transaction.debit_gl_sub_account_num,
    ar_transaction.credit_gl_dept_num,
    ar_transaction.contractual_adj_reason_code,
    ROUND(ar_transaction.transaction_code, 0, 'ROUND_HALF_EVEN') AS transaction_code,
    ar_transaction.transaction_type_code,
    ar_transaction.icn,
    ar_transaction.pe_date,
    ar_transaction.hps_order_id,
    ar_transaction.admission_date,
    ar_transaction.discharge_date
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.ar_transaction
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/armap_recoveries.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.armap_recoveries AS SELECT
    armap_recoveries.rptg_period,
    armap_recoveries.unit_num,
    armap_recoveries.coid,
    armap_recoveries.agency_name,
    armap_recoveries.agency_type,
    ROUND(armap_recoveries.recovery_amt, 3, 'ROUND_HALF_EVEN') AS recovery_amt
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.armap_recoveries
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/calculated_payor_overpayment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.calculated_payor_overpayment
   OPTIONS(description='This is a daily snapshot of Calculated  Payor Overpayment for the Legacy and Concuity accounts')
  AS SELECT
      ROUND(calculated_payor_overpayment.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      calculated_payor_overpayment.rptg_date,
      ROUND(calculated_payor_overpayment.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      calculated_payor_overpayment.iplan_id,
      calculated_payor_overpayment.coid,
      calculated_payor_overpayment.company_code,
      calculated_payor_overpayment.unit_num,
      calculated_payor_overpayment.iplan_insurance_order_num,
      ROUND(calculated_payor_overpayment.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(calculated_payor_overpayment.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ROUND(calculated_payor_overpayment.patient_person_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_person_dw_id,
      calculated_payor_overpayment.discrepancy_reason_code_1,
      calculated_payor_overpayment.discrepancy_reason_code_3,
      ROUND(calculated_payor_overpayment.cc_project_id, 0, 'ROUND_HALF_EVEN') AS cc_project_id,
      calculated_payor_overpayment.source_sid,
      calculated_payor_overpayment.late_charge_ind,
      calculated_payor_overpayment.drg_change_ind,
      calculated_payor_overpayment.multiple_pmt_ind,
      calculated_payor_overpayment.single_pmt_greater_than_total_chg_ind,
      calculated_payor_overpayment.overpayment_metric_sid,
      ROUND(calculated_payor_overpayment.potential_overpayment_amt, 3, 'ROUND_HALF_EVEN') AS potential_overpayment_amt,
      ROUND(calculated_payor_overpayment.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      calculated_payor_overpayment.source_system_code,
      calculated_payor_overpayment.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.calculated_payor_overpayment
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/claim_reprocessing_tool_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.claim_reprocessing_tool_detail
   OPTIONS(description='This table contains all detail related information derived from the claims reprocessing tool.  Data from this table is used to drive discrepancies in expected versus actual payment for a specific account.')
  AS SELECT
      ROUND(claim_reprocessing_tool_detail.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      claim_reprocessing_tool_detail.crt_log_id,
      claim_reprocessing_tool_detail.rpt_freq_type_code,
      claim_reprocessing_tool_detail.coid,
      claim_reprocessing_tool_detail.company_code,
      claim_reprocessing_tool_detail.unit_num,
      ROUND(claim_reprocessing_tool_detail.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      claim_reprocessing_tool_detail.request_type_desc,
      claim_reprocessing_tool_detail.request_date_time,
      ROUND(claim_reprocessing_tool_detail.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      claim_reprocessing_tool_detail.last_activity_date_time,
      claim_reprocessing_tool_detail.status_desc,
      claim_reprocessing_tool_detail.discrepancy_date_time,
      claim_reprocessing_tool_detail.discrepancy_source_desc,
      claim_reprocessing_tool_detail.reimbursement_impact_desc,
      claim_reprocessing_tool_detail.reprocess_reason_text,
      claim_reprocessing_tool_detail.queue_name,
      claim_reprocessing_tool_detail.claim_category_type_code,
      claim_reprocessing_tool_detail.extract_date_time,
      claim_reprocessing_tool_detail.source_system_code,
      claim_reprocessing_tool_detail.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.claim_reprocessing_tool_detail
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/cm_patient_encounter_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.cm_patient_encounter_detail
   OPTIONS(description='This table contains the information within the Case Management area related to a patient encounter for revenue cycle monitoring.')
  AS SELECT
      ROUND(cm_patient_encounter_detail.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      cm_patient_encounter_detail.midas_encounter_id,
      cm_patient_encounter_detail.company_code,
      cm_patient_encounter_detail.coid,
      ROUND(cm_patient_encounter_detail.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      cm_patient_encounter_detail.midas_acct_num,
      ROUND(cm_patient_encounter_detail.payor_dw_id_ins1, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins1,
      cm_patient_encounter_detail.iplan_id_ins1,
      cm_patient_encounter_detail.clinical_ed_greet_date,
      cm_patient_encounter_detail.clinical_ed_greet_time,
      cm_patient_encounter_detail.clinical_ed_bed_date,
      cm_patient_encounter_detail.clinical_ed_bed_time,
      cm_patient_encounter_detail.clinical_ed_triage_date,
      cm_patient_encounter_detail.clinical_ed_triage_time,
      cm_patient_encounter_detail.admission_eff_from_date,
      cm_patient_encounter_detail.total_midnight_cnt,
      cm_patient_encounter_detail.all_days_approved_ind,
      cm_patient_encounter_detail.midas_principal_payer_auth_num,
      cm_patient_encounter_detail.midas_auth_denied_pending_ind,
      cm_patient_encounter_detail.midas_principal_payer_auth_type_desc,
      cm_patient_encounter_detail.midas_date_of_denial,
      cm_patient_encounter_detail.denial_in_midas_status_desc,
      cm_patient_encounter_detail.midas_avoid_denial_day_documented_ind,
      cm_patient_encounter_detail.iq_adm_initial_rev_date_time,
      cm_patient_encounter_detail.iq_adm_reviewer_name,
      cm_patient_encounter_detail.iq_adm_reviewer_id,
      cm_patient_encounter_detail.iq_adm_rev_criteria_status,
      cm_patient_encounter_detail.iq_adm_rev_location,
      cm_patient_encounter_detail.iq_adm_rev_type_ip_ind,
      cm_patient_encounter_detail.iq_adm_rev_type_ip_med_necs_met_ind,
      cm_patient_encounter_detail.iq_adm_rev_type_ip_prior_dchg_ind,
      cm_patient_encounter_detail.iq_adm_rev_type_obs_ind,
      cm_patient_encounter_detail.iq_adm_rev_type_obs_med_necs_met_ind,
      cm_patient_encounter_detail.iq_adm_rev_type_obs_prior_dchg_ind,
      cm_patient_encounter_detail.iq_adm_rev_med_necs_desc,
      cm_patient_encounter_detail.cncr_review_cnt,
      cm_patient_encounter_detail.cncr_iq_cnt,
      cm_patient_encounter_detail.last_cncr_reviewer_name,
      cm_patient_encounter_detail.last_cncr_reviewer_id,
      cm_patient_encounter_detail.last_cncr_review_date,
      cm_patient_encounter_detail.last_cncr_review_disp_desc,
      cm_patient_encounter_detail.last_cncr_iq_reviewer_name,
      cm_patient_encounter_detail.last_cncr_iq_reviewer_id,
      cm_patient_encounter_detail.last_iq_review_code,
      cm_patient_encounter_detail.last_iq_review_criteria_met_desc,
      cm_patient_encounter_detail.last_iq_review_version_desc,
      cm_patient_encounter_detail.last_iq_review_subset_desc,
      cm_patient_encounter_detail.last_primary_review_start_date_time,
      cm_patient_encounter_detail.last_appeal_num,
      cm_patient_encounter_detail.last_appeal_date,
      cm_patient_encounter_detail.last_appeal_status_id,
      cm_patient_encounter_detail.last_appeal_status_name,
      cm_patient_encounter_detail.peer_review_status_code,
      cm_patient_encounter_detail.cm_last_no_ins_code_appl_date,
      cm_patient_encounter_detail.cm_no_ins_ind,
      cm_patient_encounter_detail.cm_last_svty_illness_code_appl_date,
      cm_patient_encounter_detail.cm_svty_illness_ind,
      cm_patient_encounter_detail.source_system_code,
      cm_patient_encounter_detail.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.cm_patient_encounter_detail
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_action_history_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.collection_action_history_dtl
   OPTIONS(description='This table maitains history of action results on the collection workflow of an account.')
  AS SELECT
      ROUND(collection_action_history_dtl.activity_history_key_num, 0, 'ROUND_HALF_EVEN') AS activity_history_key_num,
      collection_action_history_dtl.artiva_instance_code,
      ROUND(collection_action_history_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      collection_action_history_dtl.reporting_date,
      collection_action_history_dtl.action_date,
      collection_action_history_dtl.action_time,
      collection_action_history_dtl.action_code,
      collection_action_history_dtl.result_code,
      collection_action_history_dtl.action_updt_by_user_id,
      collection_action_history_dtl.action_status_code,
      collection_action_history_dtl.iplan_id,
      collection_action_history_dtl.pool_assignment_id,
      ROUND(collection_action_history_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      collection_action_history_dtl.company_code,
      collection_action_history_dtl.coid,
      collection_action_history_dtl.unit_num,
      collection_action_history_dtl.source_system_code,
      collection_action_history_dtl.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.collection_action_history_dtl
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_agency_placement_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.collection_agency_placement_detail
   OPTIONS(description='This table holds the agency placement details of all collections from Artiva  for any accounts')
  AS SELECT
      ROUND(collection_agency_placement_detail.placement_sid, 0, 'ROUND_HALF_EVEN') AS placement_sid,
      ROUND(collection_agency_placement_detail.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      collection_agency_placement_detail.company_code,
      collection_agency_placement_detail.coid,
      ROUND(collection_agency_placement_detail.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      collection_agency_placement_detail.unit_num,
      collection_agency_placement_detail.vendor_id,
      collection_agency_placement_detail.artiva_instance_code,
      collection_agency_placement_detail.placement_date,
      collection_agency_placement_detail.placement_time,
      collection_agency_placement_detail.recall_reason_code,
      collection_agency_placement_detail.recall_date,
      collection_agency_placement_detail.recall_time,
      ROUND(collection_agency_placement_detail.placement_amt, 3, 'ROUND_HALF_EVEN') AS placement_amt,
      ROUND(collection_agency_placement_detail.recall_amt, 3, 'ROUND_HALF_EVEN') AS recall_amt,
      collection_agency_placement_detail.source_system_code,
      collection_agency_placement_detail.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.collection_agency_placement_detail
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_charity_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.collection_charity_detail
   OPTIONS(description='A daily pull from Artiva system, that has the Charity data for financial review with other EDW data.')
  AS SELECT
      collection_charity_detail.reporting_date,
      ROUND(collection_charity_detail.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      collection_charity_detail.artiva_instance_code,
      collection_charity_detail.approve_deny_date,
      collection_charity_detail.charity_review_aging_date,
      collection_charity_detail.appl_approval_level_num,
      collection_charity_detail.charity_review_start_date,
      collection_charity_detail.coid,
      collection_charity_detail.company_code,
      ROUND(collection_charity_detail.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      collection_charity_detail.iplan_id,
      collection_charity_detail.appeal_ind,
      collection_charity_detail.form_1099_returned_ind,
      collection_charity_detail.charity_process_age_num,
      collection_charity_detail.charity_approve_deny_ind,
      collection_charity_detail.charity_approve_deny_user_id,
      collection_charity_detail.approval_level_1_user_id,
      collection_charity_detail.approval_level_1_date,
      collection_charity_detail.approval_level_2_user_id,
      collection_charity_detail.approval_level_2_date,
      collection_charity_detail.approval_level_3_user_id,
      collection_charity_detail.approval_level_3_date,
      collection_charity_detail.approval_level_4_user_id,
      collection_charity_detail.approval_level_4_date,
      collection_charity_detail.approval_level_5_user_id,
      collection_charity_detail.approval_level_5_date,
      collection_charity_detail.bank_statement_returned_ind,
      collection_charity_detail.appl_completed_user_id,
      collection_charity_detail.credit_report_returned_ind,
      collection_charity_detail.approval_level_num,
      ROUND(collection_charity_detail.charity_denial_amt, 3, 'ROUND_HALF_EVEN') AS charity_denial_amt,
      collection_charity_detail.denial_reason_code,
      ROUND(collection_charity_detail.discount_amt, 3, 'ROUND_HALF_EVEN') AS discount_amt,
      ROUND(collection_charity_detail.discount_pct, 3, 'ROUND_HALF_EVEN') AS discount_pct,
      collection_charity_detail.poverty_level_by_dos_ind,
      collection_charity_detail.extenuating_circumstance_code,
      collection_charity_detail.extenuating_circumstance_other_desc,
      collection_charity_detail.charity_review_end_date,
      collection_charity_detail.faa_returned_ind,
      collection_charity_detail.faa_flag_date,
      collection_charity_detail.faa_family_size_num,
      collection_charity_detail.fed_1040_returned_ind,
      ROUND(collection_charity_detail.household_income_amt, 3, 'ROUND_HALF_EVEN') AS household_income_amt,
      collection_charity_detail.unemployed_last_work_date,
      collection_charity_detail.fed_poverty_guideline_met_ind,
      collection_charity_detail.other_document_returned_ind,
      collection_charity_detail.other_document_desc,
      collection_charity_detail.charity_poverty_level_num,
      collection_charity_detail.qmb_returned_ind,
      collection_charity_detail.sa_approve_deny_flag,
      collection_charity_detail.sa_family_size_num,
      ROUND(collection_charity_detail.sa_income_amt, 3, 'ROUND_HALF_EVEN') AS sa_income_amt,
      collection_charity_detail.sa_povery_level_num,
      collection_charity_detail.sa_response_date,
      collection_charity_detail.sa_sent_date,
      collection_charity_detail.send_to_sa_value_ind,
      collection_charity_detail.state_tax_returned_ind,
      collection_charity_detail.w2_returned_ind,
      collection_charity_detail.employer_wage_return_ind,
      ROUND(collection_charity_detail.resp_party_weekly_income_amt, 3, 'ROUND_HALF_EVEN') AS resp_party_weekly_income_amt,
      ROUND(collection_charity_detail.resp_party_monthly_income_amt, 3, 'ROUND_HALF_EVEN') AS resp_party_monthly_income_amt,
      ROUND(collection_charity_detail.resp_party_yearly_income_amt, 3, 'ROUND_HALF_EVEN') AS resp_party_yearly_income_amt,
      ROUND(collection_charity_detail.spouse_weekly_income_amt, 3, 'ROUND_HALF_EVEN') AS spouse_weekly_income_amt,
      ROUND(collection_charity_detail.spouse_monthly_income_amt, 3, 'ROUND_HALF_EVEN') AS spouse_monthly_income_amt,
      ROUND(collection_charity_detail.spouse_yearly_income_amt, 3, 'ROUND_HALF_EVEN') AS spouse_yearly_income_amt,
      collection_charity_detail.source_system_code,
      collection_charity_detail.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.collection_charity_detail
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_doc_req_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.collection_doc_req_dtl
   OPTIONS(description='Collection Document Request History is maintained in this table.')
  AS SELECT
      ROUND(collection_doc_req_dtl.doc_req_key_num, 0, 'ROUND_HALF_EVEN') AS doc_req_key_num,
      collection_doc_req_dtl.artiva_instance_code,
      collection_doc_req_dtl.reporting_date,
      ROUND(collection_doc_req_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      collection_doc_req_dtl.iplan_id,
      collection_doc_req_dtl.company_code,
      collection_doc_req_dtl.coid,
      collection_doc_req_dtl.unit_num,
      ROUND(collection_doc_req_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      collection_doc_req_dtl.status_code,
      ROUND(collection_doc_req_dtl.encounter_balance_amt, 3, 'ROUND_HALF_EVEN') AS encounter_balance_amt,
      ROUND(collection_doc_req_dtl.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
      ROUND(collection_doc_req_dtl.total_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS total_adjustment_amt,
      ROUND(collection_doc_req_dtl.total_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_payment_amt,
      ROUND(collection_doc_req_dtl.current_balance_amt, 3, 'ROUND_HALF_EVEN') AS current_balance_amt,
      collection_doc_req_dtl.claim_submit_date,
      collection_doc_req_dtl.requested_date,
      collection_doc_req_dtl.response_due_date,
      collection_doc_req_dtl.approved_date,
      collection_doc_req_dtl.denied_date,
      collection_doc_req_dtl.create_date,
      collection_doc_req_dtl.sent_date,
      collection_doc_req_dtl.tracking_dtl_text,
      collection_doc_req_dtl.received_date,
      collection_doc_req_dtl.review_type_code,
      collection_doc_req_dtl.reason_text,
      collection_doc_req_dtl.othr_reason_text,
      collection_doc_req_dtl.mail_receipt_date,
      collection_doc_req_dtl.receipt_cnfm_num,
      collection_doc_req_dtl.cover_letter_reqr_ind,
      collection_doc_req_dtl.doc_delivery_method_text,
      collection_doc_req_dtl.mailed_address_sid,
      collection_doc_req_dtl.signed_by_comment_text,
      collection_doc_req_dtl.admit_order_ind,
      collection_doc_req_dtl.admit_order_aprv_ind,
      collection_doc_req_dtl.implant_inv_ind,
      collection_doc_req_dtl.implant_inv_aprv_ind,
      collection_doc_req_dtl.oth_dtl_ind,
      collection_doc_req_dtl.oth_dtl_aprv_ind,
      collection_doc_req_dtl.dchg_smry_ind,
      collection_doc_req_dtl.dchg_smry_aprv_ind,
      collection_doc_req_dtl.emrg_rpt_ind,
      collection_doc_req_dtl.emrg_rpt_aprv_ind,
      collection_doc_req_dtl.accd_dtl_ind,
      collection_doc_req_dtl.accd_dtl_aprv_ind,
      collection_doc_req_dtl.itm_bill_ind,
      collection_doc_req_dtl.itm_bill_aprv_ind,
      collection_doc_req_dtl.itm_bill_req_date,
      collection_doc_req_dtl.itm_bill_sent_date,
      collection_doc_req_dtl.first_letter_sent_date,
      collection_doc_req_dtl.second_letter_sent_date,
      collection_doc_req_dtl.third_letter_sent_date,
      collection_doc_req_dtl.sent_to_prepay_mc_ind,
      collection_doc_req_dtl.prepay_mc_sent_date,
      collection_doc_req_dtl.cplt_med_rec_ind,
      collection_doc_req_dtl.cplt_med_rec_aprv_ind,
      collection_doc_req_dtl.med_rec_req_date,
      collection_doc_req_dtl.med_rec_sent_date,
      collection_doc_req_dtl.user_id,
      collection_doc_req_dtl.user_dept_num,
      collection_doc_req_dtl.vendor_id,
      collection_doc_req_dtl.vendor_mail_comment_text,
      collection_doc_req_dtl.source_system_code,
      collection_doc_req_dtl.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.collection_doc_req_dtl
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_encounter_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.collection_encounter_detail
   OPTIONS(description='A daily inventory pull from Artiva system, that has the collections related work orders.')
  AS SELECT
      ROUND(collection_encounter_detail.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      collection_encounter_detail.reporting_date,
      collection_encounter_detail.day_interval_sid,
      collection_encounter_detail.liability_sequence_number,
      collection_encounter_detail.artiva_instance_code,
      collection_encounter_detail.company_code,
      collection_encounter_detail.coid,
      collection_encounter_detail.unit_num,
      ROUND(collection_encounter_detail.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      collection_encounter_detail.patient_type_code,
      collection_encounter_detail.account_status_code,
      collection_encounter_detail.lob_grp_code,
      collection_encounter_detail.iplan_id,
      ROUND(collection_encounter_detail.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      collection_encounter_detail.discharge_age_month_sid,
      collection_encounter_detail.last_worked_age_month_sid,
      collection_encounter_detail.delinquent_days_age_sid,
      collection_encounter_detail.encounter_financial_class_code,
      collection_encounter_detail.liability_financial_class_code,
      collection_encounter_detail.hic_claim_num,
      collection_encounter_detail.claim_num,
      collection_encounter_detail.check_num_an,
      collection_encounter_detail.medical_record_num,
      collection_encounter_detail.early_out_hold_ind,
      collection_encounter_detail.vendor_id,
      collection_encounter_detail.prev_vendor_id,
      collection_encounter_detail.liability_phase_status_desc,
      collection_encounter_detail.prev_liability_phase_desc,
      collection_encounter_detail.phase_change_date,
      collection_encounter_detail.status_desc,
      collection_encounter_detail.prior_status_desc,
      collection_encounter_detail.denial_status_code,
      collection_encounter_detail.bad_debt_reason_code,
      collection_encounter_detail.encounter_date,
      collection_encounter_detail.wait_date,
      collection_encounter_detail.denial_date,
      collection_encounter_detail.claim_submit_date,
      collection_encounter_detail.final_bill_date,
      collection_encounter_detail.early_out_date,
      collection_encounter_detail.liability_last_billed_date,
      collection_encounter_detail.liability_last_worked_date,
      collection_encounter_detail.last_worked_age_days_cnt,
      collection_encounter_detail.prev_pool_assignment_id,
      collection_encounter_detail.prev_pool_date,
      collection_encounter_detail.current_pool_assignment_id,
      collection_encounter_detail.pool_responsible_dept_name,
      collection_encounter_detail.inclusion_flag,
      collection_encounter_detail.action_code,
      collection_encounter_detail.result_code,
      collection_encounter_detail.last_action_date,
      collection_encounter_detail.last_action_time,
      collection_encounter_detail.last_action_updt_by_user_id,
      collection_encounter_detail.service_code,
      collection_encounter_detail.delinquent_days_cnt,
      ROUND(collection_encounter_detail.cc_activity_type_id, 0, 'ROUND_HALF_EVEN') AS cc_activity_type_id,
      ROUND(collection_encounter_detail.cc_prev_activity_type_id, 0, 'ROUND_HALF_EVEN') AS cc_prev_activity_type_id,
      ROUND(collection_encounter_detail.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
      ROUND(collection_encounter_detail.non_bill_charge_amt, 3, 'ROUND_HALF_EVEN') AS non_bill_charge_amt,
      ROUND(collection_encounter_detail.encounter_balance_amt, 3, 'ROUND_HALF_EVEN') AS encounter_balance_amt,
      ROUND(collection_encounter_detail.original_balance_amt, 3, 'ROUND_HALF_EVEN') AS original_balance_amt,
      ROUND(collection_encounter_detail.current_balance_amt, 3, 'ROUND_HALF_EVEN') AS current_balance_amt,
      collection_encounter_detail.liability_level_code,
      ROUND(collection_encounter_detail.total_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS total_adjustment_amt,
      ROUND(collection_encounter_detail.total_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_payment_amt,
      ROUND(collection_encounter_detail.placement_amt, 3, 'ROUND_HALF_EVEN') AS placement_amt,
      collection_encounter_detail.placement_date,
      collection_encounter_detail.placement_level_num,
      collection_encounter_detail.appeal_flag,
      collection_encounter_detail.appeal_source_system_code,
      collection_encounter_detail.denial_code,
      collection_encounter_detail.appeal_disp_code,
      collection_encounter_detail.appeal_level_num,
      collection_encounter_detail.appeal_seq_num,
      ROUND(collection_encounter_detail.appeal_source_system_id, 0, 'ROUND_HALF_EVEN') AS appeal_source_system_id,
      collection_encounter_detail.appeal_sent_date,
      collection_encounter_detail.appeal_follow_up_date,
      collection_encounter_detail.appeal_closing_date,
      ROUND(collection_encounter_detail.appeal_amt, 3, 'ROUND_HALF_EVEN') AS appeal_amt,
      collection_encounter_detail.admission_date,
      collection_encounter_detail.discharge_date,
      collection_encounter_detail.last_claim_status_rspn_date,
      collection_encounter_detail.last_claim_status_rspn_code,
      collection_encounter_detail.last_claim_status_rspn_type_code,
      collection_encounter_detail.last_claim_status_rspn_pblm_code,
      collection_encounter_detail.last_claim_status_rspn_scat_id,
      collection_encounter_detail.user_alert_comment_text,
      collection_encounter_detail.user_alert_edit_date_time,
      collection_encounter_detail.alert_created_by_user_id,
      collection_encounter_detail.user_alert_expr_date,
      collection_encounter_detail.billing_history_ind,
      ROUND(collection_encounter_detail.insgn_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS insgn_account_balance_amt,
      ROUND(collection_encounter_detail.episode_id, 0, 'ROUND_HALF_EVEN') AS episode_id,
      ROUND(collection_encounter_detail.patient_id, 0, 'ROUND_HALF_EVEN') AS patient_id,
      collection_encounter_detail.notice_of_admission_date,
      collection_encounter_detail.notice_of_election_date,
      collection_encounter_detail.erisa_ind,
      collection_encounter_detail.hosp_dc_screening_status_flag,
      collection_encounter_detail.screening_letter_include_ind,
      collection_encounter_detail.elig_program_id,
      collection_encounter_detail.elig_screening_date,
      collection_encounter_detail.web_status_flag,
      collection_encounter_detail.source_system_code,
      collection_encounter_detail.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.collection_encounter_detail
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_encounter_detail_crnt.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.collection_encounter_detail_crnt
   OPTIONS(description='A daily inventory pull from Artiva system, that has the collections related work orders which has current refreshed data.\rThis structure is the exact copy of the existing Edwpbs.Collection_Encounter_Detail')
  AS SELECT
      ROUND(collection_encounter_detail_crnt.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      collection_encounter_detail_crnt.reporting_date,
      collection_encounter_detail_crnt.day_interval_sid,
      collection_encounter_detail_crnt.liability_sequence_number,
      collection_encounter_detail_crnt.artiva_instance_code,
      collection_encounter_detail_crnt.company_code,
      collection_encounter_detail_crnt.coid,
      collection_encounter_detail_crnt.unit_num,
      ROUND(collection_encounter_detail_crnt.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      collection_encounter_detail_crnt.patient_type_code,
      collection_encounter_detail_crnt.account_status_code,
      collection_encounter_detail_crnt.lob_grp_code,
      collection_encounter_detail_crnt.iplan_id,
      ROUND(collection_encounter_detail_crnt.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      collection_encounter_detail_crnt.discharge_age_month_sid,
      collection_encounter_detail_crnt.last_worked_age_month_sid,
      collection_encounter_detail_crnt.delinquent_days_age_sid,
      collection_encounter_detail_crnt.encounter_financial_class_code,
      collection_encounter_detail_crnt.liability_financial_class_code,
      collection_encounter_detail_crnt.hic_claim_num,
      collection_encounter_detail_crnt.claim_num,
      collection_encounter_detail_crnt.check_num_an,
      collection_encounter_detail_crnt.medical_record_num,
      collection_encounter_detail_crnt.early_out_hold_ind,
      collection_encounter_detail_crnt.vendor_id,
      collection_encounter_detail_crnt.prev_vendor_id,
      collection_encounter_detail_crnt.liability_phase_status_desc,
      collection_encounter_detail_crnt.prev_liability_phase_desc,
      collection_encounter_detail_crnt.phase_change_date,
      collection_encounter_detail_crnt.status_desc,
      collection_encounter_detail_crnt.prior_status_desc,
      collection_encounter_detail_crnt.denial_status_code,
      collection_encounter_detail_crnt.bad_debt_reason_code,
      collection_encounter_detail_crnt.encounter_date,
      collection_encounter_detail_crnt.wait_date,
      collection_encounter_detail_crnt.denial_date,
      collection_encounter_detail_crnt.claim_submit_date,
      collection_encounter_detail_crnt.final_bill_date,
      collection_encounter_detail_crnt.early_out_date,
      collection_encounter_detail_crnt.liability_last_billed_date,
      collection_encounter_detail_crnt.liability_last_worked_date,
      collection_encounter_detail_crnt.last_worked_age_days_cnt,
      collection_encounter_detail_crnt.prev_pool_assignment_id,
      collection_encounter_detail_crnt.prev_pool_date,
      collection_encounter_detail_crnt.current_pool_assignment_id,
      collection_encounter_detail_crnt.pool_responsible_dept_name,
      collection_encounter_detail_crnt.inclusion_flag,
      collection_encounter_detail_crnt.action_code,
      collection_encounter_detail_crnt.result_code,
      collection_encounter_detail_crnt.last_action_date,
      collection_encounter_detail_crnt.last_action_time,
      collection_encounter_detail_crnt.last_action_updt_by_user_id,
      collection_encounter_detail_crnt.service_code,
      collection_encounter_detail_crnt.delinquent_days_cnt,
      ROUND(collection_encounter_detail_crnt.cc_activity_type_id, 0, 'ROUND_HALF_EVEN') AS cc_activity_type_id,
      ROUND(collection_encounter_detail_crnt.cc_prev_activity_type_id, 0, 'ROUND_HALF_EVEN') AS cc_prev_activity_type_id,
      ROUND(collection_encounter_detail_crnt.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
      ROUND(collection_encounter_detail_crnt.non_bill_charge_amt, 3, 'ROUND_HALF_EVEN') AS non_bill_charge_amt,
      ROUND(collection_encounter_detail_crnt.encounter_balance_amt, 3, 'ROUND_HALF_EVEN') AS encounter_balance_amt,
      ROUND(collection_encounter_detail_crnt.original_balance_amt, 3, 'ROUND_HALF_EVEN') AS original_balance_amt,
      ROUND(collection_encounter_detail_crnt.current_balance_amt, 3, 'ROUND_HALF_EVEN') AS current_balance_amt,
      collection_encounter_detail_crnt.liability_level_code,
      ROUND(collection_encounter_detail_crnt.total_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS total_adjustment_amt,
      ROUND(collection_encounter_detail_crnt.total_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_payment_amt,
      ROUND(collection_encounter_detail_crnt.placement_amt, 3, 'ROUND_HALF_EVEN') AS placement_amt,
      collection_encounter_detail_crnt.placement_date,
      collection_encounter_detail_crnt.placement_level_num,
      collection_encounter_detail_crnt.appeal_flag,
      collection_encounter_detail_crnt.appeal_source_system_code,
      collection_encounter_detail_crnt.denial_code,
      collection_encounter_detail_crnt.appeal_disp_code,
      collection_encounter_detail_crnt.appeal_level_num,
      collection_encounter_detail_crnt.appeal_seq_num,
      ROUND(collection_encounter_detail_crnt.appeal_source_system_id, 0, 'ROUND_HALF_EVEN') AS appeal_source_system_id,
      collection_encounter_detail_crnt.appeal_sent_date,
      collection_encounter_detail_crnt.appeal_follow_up_date,
      collection_encounter_detail_crnt.appeal_closing_date,
      ROUND(collection_encounter_detail_crnt.appeal_amt, 3, 'ROUND_HALF_EVEN') AS appeal_amt,
      collection_encounter_detail_crnt.admission_date,
      collection_encounter_detail_crnt.discharge_date,
      collection_encounter_detail_crnt.last_claim_status_rspn_date,
      collection_encounter_detail_crnt.last_claim_status_rspn_code,
      collection_encounter_detail_crnt.last_claim_status_rspn_type_code,
      collection_encounter_detail_crnt.last_claim_status_rspn_pblm_code,
      collection_encounter_detail_crnt.last_claim_status_rspn_scat_id,
      collection_encounter_detail_crnt.user_alert_comment_text,
      collection_encounter_detail_crnt.user_alert_edit_date_time,
      collection_encounter_detail_crnt.alert_created_by_user_id,
      collection_encounter_detail_crnt.user_alert_expr_date,
      collection_encounter_detail_crnt.billing_history_ind,
      ROUND(collection_encounter_detail_crnt.insgn_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS insgn_account_balance_amt,
      ROUND(collection_encounter_detail_crnt.episode_id, 0, 'ROUND_HALF_EVEN') AS episode_id,
      ROUND(collection_encounter_detail_crnt.patient_id, 0, 'ROUND_HALF_EVEN') AS patient_id,
      collection_encounter_detail_crnt.notice_of_admission_date,
      collection_encounter_detail_crnt.notice_of_election_date,
      collection_encounter_detail_crnt.erisa_ind,
      collection_encounter_detail_crnt.hosp_dc_screening_status_flag,
      collection_encounter_detail_crnt.screening_letter_include_ind,
      collection_encounter_detail_crnt.elig_program_id,
      collection_encounter_detail_crnt.elig_screening_date,
      collection_encounter_detail_crnt.web_status_flag,
      collection_encounter_detail_crnt.source_system_code,
      collection_encounter_detail_crnt.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.collection_encounter_detail_crnt
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_letter_request_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.collection_letter_request_list
   OPTIONS(description='Stores the lists the Artiva letters sent and to track how effective they are.')
  AS SELECT
      collection_letter_request_list.artiva_instance_code,
      ROUND(collection_letter_request_list.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      collection_letter_request_list.artiva_letter_num,
      collection_letter_request_list.artiva_encounter_num,
      collection_letter_request_list.letter_sent_date_time,
      collection_letter_request_list.letter_code,
      collection_letter_request_list.letter_name,
      collection_letter_request_list.coid,
      collection_letter_request_list.company_code,
      ROUND(collection_letter_request_list.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      collection_letter_request_list.letter_action_code,
      collection_letter_request_list.iplan_id,
      collection_letter_request_list.iplan_insurance_order_num,
      collection_letter_request_list.unit_num,
      ROUND(collection_letter_request_list.artiva_liability_num, 0, 'ROUND_HALF_EVEN') AS artiva_liability_num,
      collection_letter_request_list.free_form_ind,
      collection_letter_request_list.letter_tracking_id,
      ROUND(collection_letter_request_list.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
      collection_letter_request_list.user_id,
      collection_letter_request_list.source_system_code,
      collection_letter_request_list.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.collection_letter_request_list
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_payer_bal_threshold.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.collection_payer_bal_threshold
   OPTIONS(description='Artiva system information about  Payer Balance Threshold amounts')
  AS SELECT
      collection_payer_bal_threshold.coid,
      collection_payer_bal_threshold.company_code,
      collection_payer_bal_threshold.iplan_id,
      collection_payer_bal_threshold.artiva_instance_code,
      collection_payer_bal_threshold.unit_num,
      ROUND(collection_payer_bal_threshold.top_threshold_amt, 0, 'ROUND_HALF_EVEN') AS top_threshold_amt,
      ROUND(collection_payer_bal_threshold.high_threshold_amt, 0, 'ROUND_HALF_EVEN') AS high_threshold_amt,
      ROUND(collection_payer_bal_threshold.low_threshold_amt, 0, 'ROUND_HALF_EVEN') AS low_threshold_amt,
      ROUND(collection_payer_bal_threshold.ultra_low_threshold_amt, 0, 'ROUND_HALF_EVEN') AS ultra_low_threshold_amt,
      collection_payer_bal_threshold.source_system_code,
      collection_payer_bal_threshold.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.collection_payer_bal_threshold
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.collection_user
   OPTIONS(description='Standard Artiva Collections User file information')
  AS SELECT
      collection_user.user_id,
      collection_user.artiva_instance_code,
      collection_user.user_dept_num,
      collection_user.user_full_name,
      collection_user.global_vendor_name,
      collection_user.source_system_code,
      collection_user.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.collection_user
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/contact_savvy_acct_activity.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.contact_savvy_acct_activity
   OPTIONS(description='Account Activity Information by Persistent Time from Contact Savvy Application Add-On to Artiva system')
  AS SELECT
      ROUND(contact_savvy_acct_activity.acct_activity_id, 0, 'ROUND_HALF_EVEN') AS acct_activity_id,
      contact_savvy_acct_activity.user_on_acct_date_time,
      contact_savvy_acct_activity.user_left_acct_time,
      contact_savvy_acct_activity.artiva_instance_code,
      ROUND(contact_savvy_acct_activity.acct_activity_key_num, 0, 'ROUND_HALF_EVEN') AS acct_activity_key_num,
      contact_savvy_acct_activity.month_id,
      ROUND(contact_savvy_acct_activity.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      contact_savvy_acct_activity.dollar_strf_sid,
      ROUND(contact_savvy_acct_activity.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      contact_savvy_acct_activity.acct_liability_pool_num,
      ROUND(contact_savvy_acct_activity.liability_financial_class_code, 0, 'ROUND_HALF_EVEN') AS liability_financial_class_code,
      contact_savvy_acct_activity.company_code,
      contact_savvy_acct_activity.coid,
      contact_savvy_acct_activity.unit_num,
      ROUND(contact_savvy_acct_activity.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      contact_savvy_acct_activity.liability_sequence_num,
      contact_savvy_acct_activity.iplan_id,
      contact_savvy_acct_activity.artiva_major_payor_group_code,
      ROUND(contact_savvy_acct_activity.patient_liability_amt, 3, 'ROUND_HALF_EVEN') AS patient_liability_amt,
      ROUND(contact_savvy_acct_activity.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
      ROUND(contact_savvy_acct_activity.collection_action_cnt, 0, 'ROUND_HALF_EVEN') AS collection_action_cnt,
      contact_savvy_acct_activity.call_hold_time_scnd_amt,
      contact_savvy_acct_activity.acct_worked_ind,
      contact_savvy_acct_activity.user_on_acct_id,
      ROUND(contact_savvy_acct_activity.time_not_worked_mn_amt, 0, 'ROUND_HALF_EVEN') AS time_not_worked_mn_amt,
      ROUND(contact_savvy_acct_activity.time_worked_mn_amt, 0, 'ROUND_HALF_EVEN') AS time_worked_mn_amt,
      ROUND(contact_savvy_acct_activity.time_work_on_hold_mn_amt, 0, 'ROUND_HALF_EVEN') AS time_work_on_hold_mn_amt,
      contact_savvy_acct_activity.user_log_acct_time,
      contact_savvy_acct_activity.web_status_code,
      contact_savvy_acct_activity.pool_dialer_type_code,
      contact_savvy_acct_activity.pool_assignment_id,
      contact_savvy_acct_activity.source_system_code,
      contact_savvy_acct_activity.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.contact_savvy_acct_activity
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/contact_savvy_dialer_call_log.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.contact_savvy_dialer_call_log
   OPTIONS(description='Daily detailed Dialer Call Log from Contact Savvy Application Add-On to Artiva system.')
  AS SELECT
      ROUND(contact_savvy_dialer_call_log.call_log_key_num, 0, 'ROUND_HALF_EVEN') AS call_log_key_num,
      contact_savvy_dialer_call_log.call_initiated_date_time,
      ROUND(contact_savvy_dialer_call_log.acct_activity_id, 0, 'ROUND_HALF_EVEN') AS acct_activity_id,
      contact_savvy_dialer_call_log.artiva_instance_code,
      ROUND(contact_savvy_dialer_call_log.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      contact_savvy_dialer_call_log.call_log_pool_num,
      contact_savvy_dialer_call_log.month_id,
      contact_savvy_dialer_call_log.company_code,
      contact_savvy_dialer_call_log.coid,
      contact_savvy_dialer_call_log.unit_num,
      ROUND(contact_savvy_dialer_call_log.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ROUND(contact_savvy_dialer_call_log.dialed_phone_num, 0, 'ROUND_HALF_EVEN') AS dialed_phone_num,
      contact_savvy_dialer_call_log.call_terminated_date_time,
      contact_savvy_dialer_call_log.call_record_id,
      contact_savvy_dialer_call_log.call_talk_time_scnd_amt,
      contact_savvy_dialer_call_log.call_hold_time_scnd_amt,
      contact_savvy_dialer_call_log.call_dialed_date_time,
      contact_savvy_dialer_call_log.call_wait_time_scnd_amt,
      contact_savvy_dialer_call_log.call_update_time_scnd_amt,
      contact_savvy_dialer_call_log.call_manual_time_scnd_amt,
      contact_savvy_dialer_call_log.call_log_type_code,
      contact_savvy_dialer_call_log.pool_dialer_type_code,
      contact_savvy_dialer_call_log.voice_trak_recorded_ind,
      contact_savvy_dialer_call_log.short_call_ind,
      contact_savvy_dialer_call_log.pool_assignment_id,
      contact_savvy_dialer_call_log.file_created_by_user_id,
      contact_savvy_dialer_call_log.source_system_code,
      contact_savvy_dialer_call_log.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.contact_savvy_dialer_call_log
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/contact_savvy_multimedia_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.contact_savvy_multimedia_info
   OPTIONS(description='Multimedia information from Contact Savvy Application Add-On to Artiva system that has the Voice Trak recording information')
  AS SELECT
      ROUND(contact_savvy_multimedia_info.multimedia_key_num, 0, 'ROUND_HALF_EVEN') AS multimedia_key_num,
      contact_savvy_multimedia_info.file_create_date_time,
      ROUND(contact_savvy_multimedia_info.acct_activity_id, 0, 'ROUND_HALF_EVEN') AS acct_activity_id,
      contact_savvy_multimedia_info.artiva_instance_code,
      ROUND(contact_savvy_multimedia_info.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      contact_savvy_multimedia_info.voice_trak_pool_num,
      contact_savvy_multimedia_info.month_id,
      contact_savvy_multimedia_info.company_code,
      contact_savvy_multimedia_info.coid,
      ROUND(contact_savvy_multimedia_info.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      contact_savvy_multimedia_info.unit_num,
      contact_savvy_multimedia_info.multimedia_file_desc,
      contact_savvy_multimedia_info.multimedia_file_path_text,
      ROUND(contact_savvy_multimedia_info.call_log_key_num, 0, 'ROUND_HALF_EVEN') AS call_log_key_num,
      contact_savvy_multimedia_info.file_type_code,
      contact_savvy_multimedia_info.short_call_ind,
      contact_savvy_multimedia_info.call_log_type_code,
      contact_savvy_multimedia_info.pool_dialer_type_code,
      contact_savvy_multimedia_info.voice_trak_rec_type_code,
      ROUND(contact_savvy_multimedia_info.voice_trak_rec_time_scnd_amt, 0, 'ROUND_HALF_EVEN') AS voice_trak_rec_time_scnd_amt,
      contact_savvy_multimedia_info.file_created_by_user_id,
      contact_savvy_multimedia_info.pool_assignment_id,
      contact_savvy_multimedia_info.source_system_code,
      contact_savvy_multimedia_info.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.contact_savvy_multimedia_info
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/denial_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.denial_eom
   OPTIONS(description='Denial Month End Table captures the Denial Inventory that flows from Denial Web Apps and Concuity system.')
  AS SELECT
      denial_eom.company_code,
      denial_eom.coid,
      ROUND(denial_eom.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      ROUND(denial_eom.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      denial_eom.iplan_insurance_order_num,
      denial_eom.pe_date,
      denial_eom.denial_status_code,
      denial_eom.unit_num,
      ROUND(denial_eom.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      denial_eom.iplan_id,
      denial_eom.patient_type_code,
      ROUND(denial_eom.patient_financial_class_code, 0, 'ROUND_HALF_EVEN') AS patient_financial_class_code,
      ROUND(denial_eom.payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_code,
      denial_eom.disposition_num,
      denial_eom.appeal_origination_date,
      denial_eom.appeal_level_origination_date,
      denial_eom.appeal_closing_date,
      denial_eom.appeal_deadline_date,
      denial_eom.appeal_level_num,
      ROUND(denial_eom.beginning_balance_amt, 3, 'ROUND_HALF_EVEN') AS beginning_balance_amt,
      denial_eom.beginning_balance_count,
      ROUND(denial_eom.beginning_appeal_amt, 3, 'ROUND_HALF_EVEN') AS beginning_appeal_amt,
      ROUND(denial_eom.new_denial_account_amt, 3, 'ROUND_HALF_EVEN') AS new_denial_account_amt,
      denial_eom.new_denial_account_count,
      ROUND(denial_eom.not_true_denial_amt, 3, 'ROUND_HALF_EVEN') AS not_true_denial_amt,
      ROUND(denial_eom.write_off_denial_account_amt, 3, 'ROUND_HALF_EVEN') AS write_off_denial_account_amt,
      ROUND(denial_eom.unworked_conversion_amt, 3, 'ROUND_HALF_EVEN') AS unworked_conversion_amt,
      denial_eom.unworked_new_accounts_count,
      ROUND(denial_eom.overturned_account_amt, 3, 'ROUND_HALF_EVEN') AS overturned_account_amt,
      ROUND(denial_eom.corrections_account_amt, 3, 'ROUND_HALF_EVEN') AS corrections_account_amt,
      ROUND(denial_eom.trans_next_party_amt, 3, 'ROUND_HALF_EVEN') AS trans_next_party_amt,
      ROUND(denial_eom.ending_balance_amt, 3, 'ROUND_HALF_EVEN') AS ending_balance_amt,
      ROUND(denial_eom.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(denial_eom.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
      denial_eom.resolved_accounts_count,
      denial_eom.attending_physician_name,
      denial_eom.service_code,
      denial_eom.medical_record_num,
      denial_eom.discharge_date,
      denial_eom.last_update_id,
      denial_eom.last_update_date,
      denial_eom.work_again_date,
      denial_eom.source_system_code,
      ROUND(denial_eom.denied_charges, 3, 'ROUND_HALF_EVEN') AS denied_charges,
      ROUND(denial_eom.cc_cash_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS cc_cash_adjustment_amt,
      ROUND(denial_eom.cc_contractual_allow_adj_amt, 3, 'ROUND_HALF_EVEN') AS cc_contractual_allow_adj_amt,
      denial_eom.cc_root_cause_id,
      denial_eom.cc_denial_cat_code,
      denial_eom.cc_disposition_code,
      denial_eom.pa_vendor_code,
      denial_eom.appeal_sent_date,
      ROUND(denial_eom.cc_appeal_num, 0, 'ROUND_HALF_EVEN') AS cc_appeal_num,
      denial_eom.cc_appeal_detail_seq_num
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.denial_eom
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/denial_inventory_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.denial_inventory_list
   OPTIONS(description='This table contains all accounts being worked on in the Denial escalation Review tool ( abbreviated as DELR) both Master and Special List')
  AS SELECT
      ROUND(denial_inventory_list.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      denial_inventory_list.denial_escalation_unique_id,
      denial_inventory_list.iplan_id,
      denial_inventory_list.iplan_insurance_order_num,
      denial_inventory_list.month_id,
      denial_inventory_list.company_code,
      denial_inventory_list.coid,
      denial_inventory_list.unit_num,
      ROUND(denial_inventory_list.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      denial_inventory_list.project_date,
      denial_inventory_list.automated_letter_sent_date_time,
      ROUND(denial_inventory_list.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
      ROUND(denial_inventory_list.total_acct_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_acct_payment_amt,
      denial_inventory_list.acct_closed_ind,
      denial_inventory_list.acct_closed_date,
      denial_inventory_list.acct_closed_reason_txt,
      denial_inventory_list.assigned_attorney_name,
      denial_inventory_list.attornery_letter_date,
      denial_inventory_list.attornery_assigned_date,
      denial_inventory_list.attorney_status_code,
      denial_inventory_list.attorney_status_date,
      denial_inventory_list.claim_ref_num,
      denial_inventory_list.rebilled_ind,
      denial_inventory_list.rebill_date,
      denial_inventory_list.eligible_for_rebill_ind,
      denial_inventory_list.different_iplan_review_ind,
      denial_inventory_list.inventory_type_name,
      denial_inventory_list.source_system_code,
      denial_inventory_list.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.denial_inventory_list
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_account.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_account
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Account')
  AS SELECT
      dim_account.account_sid,
      dim_account.account_hier_name,
      dim_account.account_name_parent,
      dim_account.account_name_child,
      dim_account.account_alias_name,
      dim_account.aso_bso_storage_code,
      dim_account.sort_key_num,
      dim_account.consolidation_code,
      dim_account.storage_code,
      dim_account.two_pass_calc_code,
      dim_account.formula_text,
      dim_account.member_solve_order_num,
      dim_account.account_uda_name,
      dim_account.source_system_code,
      dim_account.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_account
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_account_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_account_status
   OPTIONS(description='Dimension table for Account Status')
  AS SELECT
      dim_account_status.account_status_sid,
      dim_account_status.aso_bso_storage_code,
      dim_account_status.account_status_name_parent,
      dim_account_status.account_status_name_child,
      dim_account_status.account_status_child_alias_name,
      dim_account_status.alias_table_name,
      dim_account_status.sort_key_num,
      dim_account_status.consolidation_code,
      dim_account_status.storage_code,
      dim_account_status.two_pass_calc_code,
      dim_account_status.formula_text,
      dim_account_status.member_solve_order_num,
      dim_account_status.account_status_hier_name
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_account_status
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_admission_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_admission_type AS SELECT
    dim_admission_type.admission_type_sid,
    dim_admission_type.aso_bso_storage_code,
    dim_admission_type.admission_type_parent,
    dim_admission_type.admission_type_child,
    dim_admission_type.admission_type_alias_name,
    dim_admission_type.sort_key_num,
    dim_admission_type.two_pass_calc_code,
    dim_admission_type.consolidation_code,
    dim_admission_type.storage_code,
    dim_admission_type.formula_text,
    dim_admission_type.member_solve_order_num,
    dim_admission_type.admission_type_hier_name,
    dim_admission_type.admission_type_name_uda
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.dim_admission_type
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_age.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_age AS SELECT
    dim_age.age_sid,
    dim_age.aso_bso_storage_code,
    dim_age.age_name_parent,
    dim_age.age_name_child,
    dim_age.age_alias_name,
    dim_age.alias_table_name,
    dim_age.sort_key_num,
    dim_age.two_pass_calc_code,
    dim_age.consolidation_code,
    dim_age.storage_code,
    dim_age.formula_text,
    dim_age.member_solve_order_num,
    dim_age.age_hier_name,
    dim_age.age_name_1_uda,
    dim_age.age_name_2_uda
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.dim_age
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_appeal_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_appeal_code
   OPTIONS(description='Dimension table that captures the hierarchy and descriptions of various Appeals in the denial resolution process')
  AS SELECT
      dim_appeal_code.appeal_code_sid,
      dim_appeal_code.aso_bso_storage_code,
      dim_appeal_code.appeal_code_parent,
      dim_appeal_code.appeal_code_child,
      dim_appeal_code.appeal_code_alias_name,
      dim_appeal_code.sort_key_num,
      dim_appeal_code.two_pass_calc_code,
      dim_appeal_code.consolidation_code,
      dim_appeal_code.storage_code,
      dim_appeal_code.formula_text,
      dim_appeal_code.member_solve_order_num,
      dim_appeal_code.appeal_code_hier_name,
      dim_appeal_code.appeal_code_uda_name
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_appeal_code
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_appeal_disp.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_appeal_disp AS SELECT
    dim_appeal_disp.appeal_disp_sid,
    dim_appeal_disp.aso_bso_storage_code,
    dim_appeal_disp.appeal_disp_parent,
    dim_appeal_disp.appeal_disp_child,
    dim_appeal_disp.appeal_disp_alias_name,
    dim_appeal_disp.sort_key_num,
    dim_appeal_disp.two_pass_calc_code,
    dim_appeal_disp.consolidation_code,
    dim_appeal_disp.storage_code,
    dim_appeal_disp.formula_text,
    dim_appeal_disp.member_solve_order_num,
    dim_appeal_disp.appeal_disp_hier_name,
    dim_appeal_disp.appeal_disp_uda_name
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.dim_appeal_disp
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_appeal_root_cause.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_appeal_root_cause AS SELECT
    ROUND(dim_appeal_root_cause.appeal_root_cause_sid, 0, 'ROUND_HALF_EVEN') AS appeal_root_cause_sid,
    dim_appeal_root_cause.aso_bso_storage_code,
    dim_appeal_root_cause.appeal_root_cause_parent,
    dim_appeal_root_cause.appeal_root_cause_child,
    dim_appeal_root_cause.appeal_root_cause_alias_name,
    dim_appeal_root_cause.sort_key_num,
    dim_appeal_root_cause.two_pass_calc_code,
    dim_appeal_root_cause.consolidation_code,
    dim_appeal_root_cause.storage_code,
    dim_appeal_root_cause.formula_text,
    dim_appeal_root_cause.member_solve_order_num,
    dim_appeal_root_cause.appeal_root_cause_hier_name,
    dim_appeal_root_cause.appeal_root_cause_uda_name
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.dim_appeal_root_cause
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_organization.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_bdgt_organization
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Organization')
  AS SELECT
      dim_bdgt_organization.organization_sid,
      dim_bdgt_organization.organization_hier_name,
      dim_bdgt_organization.organization_name_parent,
      dim_bdgt_organization.organization_name_child,
      dim_bdgt_organization.organization_alias_name,
      dim_bdgt_organization.aso_bso_storage_code,
      dim_bdgt_organization.sort_key_num,
      dim_bdgt_organization.consolidation_code,
      dim_bdgt_organization.storage_code,
      dim_bdgt_organization.two_pass_calc_code,
      dim_bdgt_organization.formula_text,
      dim_bdgt_organization.member_solve_order_num,
      dim_bdgt_organization.organization_uda_name,
      dim_bdgt_organization.source_system_code,
      dim_bdgt_organization.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_bdgt_organization
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_scenario.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_bdgt_scenario
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Scenario')
  AS SELECT
      dim_bdgt_scenario.scenario_sid,
      dim_bdgt_scenario.scenario_hier_name,
      dim_bdgt_scenario.scenario_name_parent,
      dim_bdgt_scenario.scenario_name_child,
      dim_bdgt_scenario.scenario_alias_name,
      dim_bdgt_scenario.aso_bso_storage_code,
      dim_bdgt_scenario.sort_key_num,
      dim_bdgt_scenario.consolidation_code,
      dim_bdgt_scenario.storage_code,
      dim_bdgt_scenario.two_pass_calc_code,
      dim_bdgt_scenario.formula_text,
      dim_bdgt_scenario.member_solve_order_num,
      dim_bdgt_scenario.scenario_uda_name,
      dim_bdgt_scenario.source_system_code,
      dim_bdgt_scenario.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_bdgt_scenario
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_time.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_bdgt_time
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of a Time within the Year')
  AS SELECT
      dim_bdgt_time.time_sid,
      dim_bdgt_time.time_hier_name,
      dim_bdgt_time.time_name_parent,
      dim_bdgt_time.time_name_child,
      dim_bdgt_time.time_alias_name,
      dim_bdgt_time.aso_bso_storage_code,
      dim_bdgt_time.sort_key_num,
      dim_bdgt_time.consolidation_code,
      dim_bdgt_time.storage_code,
      dim_bdgt_time.two_pass_calc_code,
      dim_bdgt_time.formula_text,
      dim_bdgt_time.member_solve_order_num,
      dim_bdgt_time.time_uda_name,
      dim_bdgt_time.source_system_code,
      dim_bdgt_time.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_bdgt_time
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_value.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_bdgt_value
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Value')
  AS SELECT
      dim_bdgt_value.value_sid,
      dim_bdgt_value.value_hier_name,
      dim_bdgt_value.value_name_parent,
      dim_bdgt_value.value_name_child,
      dim_bdgt_value.value_alias_name,
      dim_bdgt_value.aso_bso_storage_code,
      dim_bdgt_value.sort_key_num,
      dim_bdgt_value.consolidation_code,
      dim_bdgt_value.storage_code,
      dim_bdgt_value.two_pass_calc_code,
      dim_bdgt_value.formula_text,
      dim_bdgt_value.member_solve_order_num,
      dim_bdgt_value.value_uda_name,
      dim_bdgt_value.source_system_code,
      dim_bdgt_value.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_bdgt_value
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_year.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_bdgt_year
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Year')
  AS SELECT
      dim_bdgt_year.year_sid,
      dim_bdgt_year.year_hier_name,
      dim_bdgt_year.year_name_parent,
      dim_bdgt_year.year_name_child,
      dim_bdgt_year.year_alias_name,
      dim_bdgt_year.aso_bso_storage_code,
      dim_bdgt_year.sort_key_num,
      dim_bdgt_year.consolidation_code,
      dim_bdgt_year.storage_code,
      dim_bdgt_year.two_pass_calc_code,
      dim_bdgt_year.formula_text,
      dim_bdgt_year.member_solve_order_num,
      dim_bdgt_year.year_uda_name,
      dim_bdgt_year.source_system_code,
      dim_bdgt_year.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_bdgt_year
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_credit_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_credit_status
   OPTIONS(description='Dimension table for Credit Status')
  AS SELECT
      dim_credit_status.credit_status_sid,
      dim_credit_status.aso_bso_storage_code,
      dim_credit_status.credit_status_name_parent,
      dim_credit_status.credit_status_name_child,
      dim_credit_status.credit_status_child_alias_name,
      dim_credit_status.alias_table_name,
      dim_credit_status.sort_key_num,
      dim_credit_status.consolidation_code,
      dim_credit_status.storage_code,
      dim_credit_status.two_pass_calc_code,
      dim_credit_status.formula_text,
      dim_credit_status.member_solve_order_num,
      dim_credit_status.credit_status_hier_name
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_credit_status
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_denial_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_denial_type AS SELECT
    dim_denial_type.denial_type_sid,
    dim_denial_type.aso_bso_storage_code,
    dim_denial_type.denial_type_parent,
    dim_denial_type.denial_type_child,
    dim_denial_type.denial_type_alias_name,
    dim_denial_type.alias_table_name,
    dim_denial_type.sort_key_num,
    dim_denial_type.two_pass_calc_code,
    dim_denial_type.consolidation_code,
    dim_denial_type.storage_code,
    dim_denial_type.formula_text,
    dim_denial_type.member_solve_order_num,
    dim_denial_type.denial_type_hier_name,
    dim_denial_type.denial_type_name_uda
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.dim_denial_type
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_department.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_department
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels in Departments')
  AS SELECT
      dim_department.department_sid,
      dim_department.department_hier_name,
      dim_department.department_name_parent,
      dim_department.department_name_child,
      dim_department.department_alias_name,
      dim_department.aso_bso_storage_code,
      dim_department.sort_key_num,
      dim_department.consolidation_code,
      dim_department.storage_code,
      dim_department.two_pass_calc_code,
      dim_department.formula_text,
      dim_department.member_solve_order_num,
      dim_department.department_uda_name,
      dim_department.source_system_code,
      dim_department.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_department
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_discrepancy_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_discrepancy_type AS SELECT
    dim_discrepancy_type.discrepancy_type_sid,
    dim_discrepancy_type.aso_bso_storage_code,
    dim_discrepancy_type.discrepancy_type_name_parent,
    dim_discrepancy_type.discrepancy_type_name_child,
    dim_discrepancy_type.discrepancy_type_alias_name,
    dim_discrepancy_type.alias_table_name,
    dim_discrepancy_type.sort_key_num,
    dim_discrepancy_type.two_pass_calc_code,
    dim_discrepancy_type.consolidation_code,
    dim_discrepancy_type.storage_code,
    dim_discrepancy_type.formula_text,
    dim_discrepancy_type.member_solve_order_num,
    dim_discrepancy_type.discrepancy_type_hier_name,
    dim_discrepancy_type.discrepancy_type_uda
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.dim_discrepancy_type
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_dollar_stratification.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_dollar_stratification
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels in Dollar Amount Stratification')
  AS SELECT
      dim_dollar_stratification.dollar_strf_sid,
      dim_dollar_stratification.dollar_strf_hier_name,
      dim_dollar_stratification.aso_bso_storage_code,
      dim_dollar_stratification.dollar_strf_parent,
      dim_dollar_stratification.dollar_strf_child,
      dim_dollar_stratification.dollar_strf_alias_name,
      dim_dollar_stratification.sort_key_num,
      dim_dollar_stratification.two_pass_calc_code,
      dim_dollar_stratification.consolidation_code,
      dim_dollar_stratification.storage_code,
      dim_dollar_stratification.formula_text,
      dim_dollar_stratification.member_solve_order_num,
      dim_dollar_stratification.dollar_strf_uda_name
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_dollar_stratification
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_employee.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_employee
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Employee')
  AS SELECT
      dim_employee.employee_sid,
      dim_employee.employee_hier_name,
      dim_employee.employee_name_parent,
      dim_employee.employee_name_child,
      dim_employee.employee_alias_name,
      dim_employee.aso_bso_storage_code,
      dim_employee.sort_key_num,
      dim_employee.consolidation_code,
      dim_employee.storage_code,
      dim_employee.two_pass_calc_code,
      dim_employee.formula_text,
      dim_employee.member_solve_order_num,
      dim_employee.employee_uda_name,
      dim_employee.source_system_code,
      dim_employee.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_employee
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_esb_organization.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_esb_organization
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Organization for Essbase Analysis')
  AS SELECT
      CASE
        WHEN upper(dim_esb_organization.min_unit_sid_ind) = 'Y' THEN CASE
           substr(dim_esb_organization.org_name_child, 2, 5)
          WHEN '' THEN 0
          ELSE CAST(substr(dim_esb_organization.org_name_child, 2, 5) as INT64)
        END
        ELSE dim_esb_organization.org_sid
      END AS org_sid,
      dim_esb_organization.lob_sid,
      dim_esb_organization.same_store_sid,
      dim_esb_organization.coid,
      dim_esb_organization.company_code,
      dim_esb_organization.aso_bso_storage_code,
      dim_esb_organization.org_name_parent,
      dim_esb_organization.org_name_child,
      dim_esb_organization.org_alias_name,
      dim_esb_organization.org_coid_alias_name,
      dim_esb_organization.alias_table_name,
      dim_esb_organization.sort_key_num,
      dim_esb_organization.consolidation_code,
      dim_esb_organization.storage_code,
      dim_esb_organization.two_pass_calc_code,
      dim_esb_organization.formula_text,
      dim_esb_organization.member_solve_order_num,
      dim_esb_organization.org_level_uda_name,
      dim_esb_organization.org_hier_name,
      dim_esb_organization.source_system_code,
      dim_esb_organization.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_esb_organization
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_los.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_los AS SELECT
    dim_los.los_sid,
    dim_los.aso_bso_storage_code,
    dim_los.los_name_parent,
    dim_los.los_name_child,
    dim_los.los_alias_name,
    dim_los.alias_table_name,
    dim_los.sort_key_num,
    dim_los.two_pass_calc_code,
    dim_los.consolidation_code,
    dim_los.storage_code,
    dim_los.formula_text,
    dim_los.member_solve_order_num,
    dim_los.los_hier_name
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.dim_los
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_metric
   OPTIONS(description='Generic Dimension table')
  AS SELECT
      dim_metric.metric_sid,
      dim_metric.metric_hier_name,
      dim_metric.metric_name_parent,
      dim_metric.metric_name_child,
      dim_metric.metric_alias_name,
      dim_metric.aso_bso_storage_code,
      dim_metric.sort_key_num,
      dim_metric.consolidation_code,
      dim_metric.storage_code,
      dim_metric.two_pass_calc_code,
      dim_metric.formula_text,
      dim_metric.member_solve_order_num
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_metric
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_payor AS SELECT
    dim_payor.payor_sid,
    dim_payor.payor_id,
    dim_payor.payor_name,
    dim_payor.payor_short_name,
    dim_payor.payor_type,
    dim_payor.dw_last_update_date_time,
    dim_payor.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.dim_payor
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_pbs_drg.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_pbs_drg
   OPTIONS(description='Dimension table to capture Diagnosis related groupin with Medical and Surgical Grouping')
  AS SELECT
      dim_pbs_drg.ms_drg_sid,
      dim_pbs_drg.aso_bso_storage_code,
      dim_pbs_drg.ms_drg_name_parent,
      dim_pbs_drg.ms_drg_name_child,
      dim_pbs_drg.ms_drg_child_alias_name,
      dim_pbs_drg.alias_table_name,
      dim_pbs_drg.sort_key_num,
      dim_pbs_drg.consolidation_code,
      dim_pbs_drg.storage_code,
      dim_pbs_drg.two_pass_calc_code,
      dim_pbs_drg.formula_text,
      dim_pbs_drg.ms_drg_med_surg_code,
      dim_pbs_drg.ms_drg_med_surg_name,
      dim_pbs_drg.ms_chois_prod_line_code,
      dim_pbs_drg.ms_chois_prod_line_desc,
      dim_pbs_drg.member_solve_order_num,
      dim_pbs_drg.ms_drg_hier_name
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_pbs_drg
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_rcm_org_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_rcm_org_level AS SELECT
    dim_rcm_org_level.service_type_name,
    dim_rcm_org_level.fact_lvl_code,
    dim_rcm_org_level.child_fact_lvl_code,
    dim_rcm_org_level.parent_code,
    dim_rcm_org_level.child_code,
    dim_rcm_org_level.parent_desc,
    dim_rcm_org_level.child_desc,
    dim_rcm_org_level.dw_last_update_date_time,
    dim_rcm_org_level.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.dim_rcm_org_level
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_rcm_organization.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_rcm_organization
   OPTIONS(description='Organization dimension table for VI dashboards.')
  AS SELECT
      dim_rcm_organization.company_code,
      dim_rcm_organization.coid,
      dim_rcm_organization.customer_code,
      dim_rcm_organization.customer_short_name,
      dim_rcm_organization.customer_name,
      dim_rcm_organization.ssc_code,
      dim_rcm_organization.unit_num,
      dim_rcm_organization.facility_mnemonic,
      dim_rcm_organization.group_code,
      dim_rcm_organization.division_code,
      dim_rcm_organization.market_code,
      dim_rcm_organization.f_level,
      dim_rcm_organization.partnership_ind,
      dim_rcm_organization.go_live_date,
      dim_rcm_organization.eff_from_date,
      dim_rcm_organization.eff_to_date,
      dim_rcm_organization.ssc_alias_code,
      dim_rcm_organization.division_alias_code,
      dim_rcm_organization.ssc_name,
      dim_rcm_organization.ssc_alias_name,
      dim_rcm_organization.corporate_name,
      dim_rcm_organization.group_name,
      dim_rcm_organization.market_name,
      dim_rcm_organization.division_name,
      dim_rcm_organization.group_alias_name,
      dim_rcm_organization.market_alias_name,
      dim_rcm_organization.division_alias_name,
      dim_rcm_organization.ssc_coid,
      dim_rcm_organization.coid_name,
      dim_rcm_organization.facility_name,
      dim_rcm_organization.facility_state_code,
      dim_rcm_organization.medicare_expansion_ind,
      dim_rcm_organization.medicaid_conversion_vendor_name,
      dim_rcm_organization.facility_close_date,
      dim_rcm_organization.his_vendor_name,
      dim_rcm_organization.rcps_migration_date,
      ROUND(dim_rcm_organization.discrepancy_threshold_amt, 3, 'ROUND_HALF_EVEN') AS discrepancy_threshold_amt,
      ROUND(dim_rcm_organization.sma_high_dollar_threshold_amt, 3, 'ROUND_HALF_EVEN') AS sma_high_dollar_threshold_amt,
      dim_rcm_organization.hsc_member_ind,
      dim_rcm_organization.clear_contract_ind,
      dim_rcm_organization.client_outbound_ind,
      dim_rcm_organization.him_conversion_date,
      dim_rcm_organization.summ_days_release_date
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_rcm_organization
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_source.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_source
   OPTIONS(description='Dimension table that captures the Source systems currently used in PARS hierarchy')
  AS SELECT
      dim_source.source_sid,
      dim_source.source_hier_name,
      dim_source.aso_bso_storage_code,
      dim_source.source_parent,
      dim_source.source_child,
      dim_source.source_alias_name,
      dim_source.sort_key_num,
      dim_source.two_pass_calc_code,
      dim_source.consolidation_code,
      dim_source.storage_code,
      dim_source.formula_text,
      dim_source.member_solve_order_num,
      dim_source.source_uda_name,
      dim_source.source_system_code,
      dim_source.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.dim_source
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/discrepancy_inventory_analysis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.discrepancy_inventory_analysis
   OPTIONS(description='Monthly Discrepancy Inventory for all customers to evaluate the Cost year logic calculation')
  AS SELECT
      discrepancy_inventory_analysis.date_sid,
      ROUND(discrepancy_inventory_analysis.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      ROUND(discrepancy_inventory_analysis.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      discrepancy_inventory_analysis.iplan_insurance_order_num,
      discrepancy_inventory_analysis.log_id,
      discrepancy_inventory_analysis.ar_bill_thru_date,
      discrepancy_inventory_analysis.discrepancy_creation_date,
      ROUND(discrepancy_inventory_analysis.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      discrepancy_inventory_analysis.payor_sequence_sid,
      discrepancy_inventory_analysis.unit_num_sid,
      discrepancy_inventory_analysis.eor_log_date,
      discrepancy_inventory_analysis.log_sequence_num,
      discrepancy_inventory_analysis.year_created_sid,
      discrepancy_inventory_analysis.cost_year_sid,
      discrepancy_inventory_analysis.cost_year_end_date,
      discrepancy_inventory_analysis.payor_financial_class_sid,
      discrepancy_inventory_analysis.payor_sid,
      discrepancy_inventory_analysis.patient_type_sid,
      discrepancy_inventory_analysis.discrepancy_age_month_sid,
      discrepancy_inventory_analysis.discharge_age_month_sid,
      discrepancy_inventory_analysis.scenario_sid,
      ROUND(discrepancy_inventory_analysis.reason_code_1_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_1_sid,
      ROUND(discrepancy_inventory_analysis.reason_code_2_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_2_sid,
      ROUND(discrepancy_inventory_analysis.reason_code_3_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_3_sid,
      ROUND(discrepancy_inventory_analysis.reason_code_4_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_4_sid,
      discrepancy_inventory_analysis.source_sid,
      ROUND(discrepancy_inventory_analysis.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id,
      discrepancy_inventory_analysis.coid,
      discrepancy_inventory_analysis.company_code,
      discrepancy_inventory_analysis.resolve_reason_code,
      discrepancy_inventory_analysis.discharge_date,
      discrepancy_inventory_analysis.ra_remit_date,
      discrepancy_inventory_analysis.reason_assignment_date_1,
      discrepancy_inventory_analysis.reason_assignment_date_2,
      discrepancy_inventory_analysis.reason_assignment_date_3,
      discrepancy_inventory_analysis.reason_assignment_date_4,
      ROUND(discrepancy_inventory_analysis.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
      discrepancy_inventory_analysis.primary_reason_code_change_cnt,
      discrepancy_inventory_analysis.discrepancy_days,
      discrepancy_inventory_analysis.discrepancy_resolved_date,
      discrepancy_inventory_analysis.discrepany_type_code,
      discrepancy_inventory_analysis.discrepancy_type_sid,
      discrepancy_inventory_analysis.same_store_sid,
      discrepancy_inventory_analysis.dollar_strtf_sid,
      ROUND(discrepancy_inventory_analysis.var_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_beg_amt,
      ROUND(discrepancy_inventory_analysis.var_new_amt, 3, 'ROUND_HALF_EVEN') AS var_new_amt,
      ROUND(discrepancy_inventory_analysis.var_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_resolved_amt,
      ROUND(discrepancy_inventory_analysis.var_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_othr_cor_amt,
      ROUND(discrepancy_inventory_analysis.var_end_amt, 3, 'ROUND_HALF_EVEN') AS var_end_amt,
      ROUND(discrepancy_inventory_analysis.exp_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_crnt_amt,
      ROUND(discrepancy_inventory_analysis.exp_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_rate_amt,
      discrepancy_inventory_analysis.var_beg_cnt,
      discrepancy_inventory_analysis.var_new_cnt,
      discrepancy_inventory_analysis.var_resolved_cnt,
      discrepancy_inventory_analysis.var_othr_cor_cnt,
      discrepancy_inventory_analysis.var_end_cnt,
      discrepancy_inventory_analysis.exp_crnt_cnt,
      discrepancy_inventory_analysis.exp_rate_cnt,
      discrepancy_inventory_analysis.resolved_days,
      discrepancy_inventory_analysis.end_days,
      discrepancy_inventory_analysis.source_system_code,
      discrepancy_inventory_analysis.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.discrepancy_inventory_analysis
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_appeal_root_cause_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.eis_appeal_root_cause_dim AS SELECT
    ROUND(eis_appeal_root_cause_dim.appeal_root_cause_sid, 0, 'ROUND_HALF_EVEN') AS appeal_root_cause_sid,
    eis_appeal_root_cause_dim.appeal_root_cause_gen01,
    eis_appeal_root_cause_dim.appeal_root_cause_gen01_alias,
    eis_appeal_root_cause_dim.appeal_root_cause_gen01a,
    eis_appeal_root_cause_dim.appeal_root_cause_gen01a_alias,
    eis_appeal_root_cause_dim.appeal_root_cause_gen02,
    eis_appeal_root_cause_dim.appeal_root_cause_gen02_alias,
    eis_appeal_root_cause_dim.appeal_root_cause_gen03,
    eis_appeal_root_cause_dim.appeal_root_cause_gen03_alias
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.eis_appeal_root_cause_dim
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_dcrp_unit_cost_year_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.eis_dcrp_unit_cost_year_dim AS SELECT
    eis_dcrp_unit_cost_year_dim.unit_num_sid,
    eis_dcrp_unit_cost_year_dim.year_created_sid,
    eis_dcrp_unit_cost_year_dim.source_sid,
    eis_dcrp_unit_cost_year_dim.company_code,
    eis_dcrp_unit_cost_year_dim.coid,
    eis_dcrp_unit_cost_year_dim.cost_year_beg_date,
    eis_dcrp_unit_cost_year_dim.cost_year_end_date
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.eis_dcrp_unit_cost_year_dim
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_dollar_strf_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.eis_dollar_strf_dim AS SELECT
    eis_dollar_strf_dim.dollar_strf_sid,
    eis_dollar_strf_dim.dollar_strf_member,
    eis_dollar_strf_dim.dollar_strf_alias,
    eis_dollar_strf_dim.dollar_strf_sort,
    eis_dollar_strf_dim.dollar_strf_gen02,
    eis_dollar_strf_dim.dollar_strf_gen02_alias,
    eis_dollar_strf_dim.dollar_strf_gen02_info,
    eis_dollar_strf_dim.dollar_strf_gen02_sort,
    eis_dollar_strf_dim.dollar_strf_gen03,
    eis_dollar_strf_dim.dollar_strf_gen03_alias,
    eis_dollar_strf_dim.dollar_strf_gen03_info,
    eis_dollar_strf_dim.dollar_strf_gen03_sort,
    eis_dollar_strf_dim.dollar_strf_gen04,
    eis_dollar_strf_dim.dollar_strf_gen04_alias,
    eis_dollar_strf_dim.dollar_strf_gen04_info,
    eis_dollar_strf_dim.dollar_strf_gen04_sort,
    eis_dollar_strf_dim.dollar_strf_gen05,
    eis_dollar_strf_dim.dollar_strf_gen05_alias,
    eis_dollar_strf_dim.dollar_strf_gen05_info,
    eis_dollar_strf_dim.dollar_strf_gen05_sort
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.eis_dollar_strf_dim
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_payor_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.eis_payor_dim
   OPTIONS(description='Copy of Payor Dimension from EDWPF.')
  AS SELECT
      eis_payor_dim.payor_sid,
      eis_payor_dim.payor_member,
      eis_payor_dim.payor_alias,
      eis_payor_dim.payor_gen02,
      eis_payor_dim.payor_gen02_sort_id,
      eis_payor_dim.payor_gen02_info,
      eis_payor_dim.payor_gen02_alias,
      eis_payor_dim.payor_gen03,
      eis_payor_dim.payor_gen03_sort_id,
      eis_payor_dim.payor_gen03_info,
      eis_payor_dim.payor_gen03_alias,
      eis_payor_dim.payor_gen04,
      eis_payor_dim.payor_gen04_sort_id,
      eis_payor_dim.payor_gen04_info,
      eis_payor_dim.payor_gen04_alias,
      eis_payor_dim.payor_gen05,
      eis_payor_dim.payor_gen05_sort_id,
      eis_payor_dim.payor_gen05_info,
      eis_payor_dim.payor_gen05_alias,
      eis_payor_dim.data_exists_flag01,
      eis_payor_dim.data_exists_flag02,
      eis_payor_dim.data_exists_flag03,
      eis_payor_dim.data_exists_flag04,
      eis_payor_dim.data_exists_flag05,
      eis_payor_dim.data_exists_flag06,
      eis_payor_dim.data_exists_flag07,
      eis_payor_dim.data_exists_flag08,
      eis_payor_dim.data_exists_flag09,
      eis_payor_dim.data_exists_flag10
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.eis_payor_dim
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_reason_code_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.eis_reason_code_dim AS SELECT
    ROUND(eis_reason_code_dim.reason_code_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_sid,
    eis_reason_code_dim.reason_code_member,
    eis_reason_code_dim.reason_code_alias,
    eis_reason_code_dim.reason_code_sort,
    eis_reason_code_dim.reason_code_gen02,
    eis_reason_code_dim.reason_code_gen02_alias,
    eis_reason_code_dim.reason_code_gen02_info,
    eis_reason_code_dim.reason_code_gen02_sort,
    eis_reason_code_dim.reason_code_gen03,
    eis_reason_code_dim.reason_code_gen03_alias,
    eis_reason_code_dim.reason_code_gen03_info,
    eis_reason_code_dim.reason_code_gen03_sort,
    eis_reason_code_dim.reason_code_gen04,
    eis_reason_code_dim.reason_code_gen04_alias,
    eis_reason_code_dim.reason_code_gen04_info,
    eis_reason_code_dim.reason_code_gen04_sort,
    eis_reason_code_dim.reason_code_gen05,
    eis_reason_code_dim.reason_code_gen05_alias,
    eis_reason_code_dim.reason_code_gen05_info,
    eis_reason_code_dim.reason_code_gen05_sort,
    eis_reason_code_dim.reason_code_gen06,
    eis_reason_code_dim.reason_code_gen06_alias,
    eis_reason_code_dim.reason_code_gen06_info,
    eis_reason_code_dim.reason_code_gen06_sort
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.eis_reason_code_dim
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_scenario_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.eis_scenario_dim
   OPTIONS(description='Dimension table that captures the Formula and descriptions of various Scenario members for Essbase Cubes')
  AS SELECT
      eis_scenario_dim.scenario_sid,
      eis_scenario_dim.scenario_member,
      eis_scenario_dim.scenario_alias,
      eis_scenario_dim.scenario_info,
      eis_scenario_dim.scenario_formula,
      eis_scenario_dim.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.eis_scenario_dim
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/erequest_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.erequest_inventory
   OPTIONS(description='Daily Inventory of ERequest information to monitor the billing cycle.')
  AS SELECT
      erequest_inventory.rptg_date,
      erequest_inventory.erequest_id,
      erequest_inventory.claim_id,
      erequest_inventory.coid,
      erequest_inventory.erequest_user_department_num,
      ROUND(erequest_inventory.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      ROUND(erequest_inventory.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      erequest_inventory.iplan_id,
      ROUND(erequest_inventory.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      ROUND(erequest_inventory.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      erequest_inventory.patient_type_code_pos1,
      erequest_inventory.company_code,
      erequest_inventory.discharge_date,
      erequest_inventory.admission_date,
      erequest_inventory.bill_type_code,
      erequest_inventory.med_rec_num,
      erequest_inventory.account_status_sid,
      erequest_inventory.vendor_id,
      erequest_inventory.final_queue_dept_id,
      erequest_inventory.current_queue_dept_id,
      erequest_inventory.previous_queue_dept_id,
      erequest_inventory.queue_changed_ind,
      erequest_inventory.provider_npi,
      erequest_inventory.unbilled_reason_code,
      ROUND(erequest_inventory.claim_charge_amt, 3, 'ROUND_HALF_EVEN') AS claim_charge_amt,
      ROUND(erequest_inventory.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(erequest_inventory.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      erequest_inventory.request_type_id,
      erequest_inventory.request_created_by_user_id,
      erequest_inventory.request_status_code,
      erequest_inventory.concuity_acct_ind,
      erequest_inventory.payer_claim_control_id,
      erequest_inventory.service_code,
      erequest_inventory.service_from_date,
      erequest_inventory.service_to_date,
      erequest_inventory.request_created_date,
      erequest_inventory.last_activity_date,
      erequest_inventory.last_activity_by_user_id,
      erequest_inventory.last_queue_change_date,
      erequest_inventory.final_queue_change_date,
      erequest_inventory.final_queue_changed_by_user_id,
      erequest_inventory.escalation_override_date,
      erequest_inventory.claim_submit_date,
      erequest_inventory.claim_download_date,
      erequest_inventory.erequest_src_sys_sid,
      erequest_inventory.purge_ind,
      erequest_inventory.new_req_ind,
      erequest_inventory.updated_req_ind,
      erequest_inventory.final_diagnosis_flg,
      erequest_inventory.source_system_code,
      erequest_inventory.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.erequest_inventory
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/erequest_productivity_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.erequest_productivity_dtl
   OPTIONS(description='This table helps business users manage and analyze productivity for SSC staff as they work through the revenue cycle operations.')
  AS SELECT
      erequest_productivity_dtl.erequest_id,
      erequest_productivity_dtl.note_date_time,
      ROUND(erequest_productivity_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      erequest_productivity_dtl.iplan_id,
      ROUND(erequest_productivity_dtl.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      erequest_productivity_dtl.coid,
      ROUND(erequest_productivity_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      erequest_productivity_dtl.patient_type_code_pos1,
      erequest_productivity_dtl.company_code,
      erequest_productivity_dtl.discharge_date,
      erequest_productivity_dtl.admission_date,
      erequest_productivity_dtl.from_queue_dept_id,
      erequest_productivity_dtl.to_queue_dept_id,
      erequest_productivity_dtl.unbilled_reason_code,
      erequest_productivity_dtl.user_action_type_code,
      erequest_productivity_dtl.request_created_date,
      ROUND(erequest_productivity_dtl.claim_charge_amt, 3, 'ROUND_HALF_EVEN') AS claim_charge_amt,
      ROUND(erequest_productivity_dtl.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      erequest_productivity_dtl.notes_desc,
      erequest_productivity_dtl.user_id,
      erequest_productivity_dtl.user_full_name,
      erequest_productivity_dtl.source_system_code,
      erequest_productivity_dtl.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.erequest_productivity_dtl
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/facility_dimension_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.facility_dimension_eom
   OPTIONS(description='Facility Dimension End of Month Snapshot')
  AS SELECT
      facility_dimension_eom.company_code,
      facility_dimension_eom.coid,
      facility_dimension_eom.pe_date,
      facility_dimension_eom.unit_num,
      facility_dimension_eom.coid_name,
      facility_dimension_eom.c_level,
      facility_dimension_eom.corporate_name,
      facility_dimension_eom.s_level,
      facility_dimension_eom.sector_name,
      facility_dimension_eom.b_level,
      facility_dimension_eom.group_name,
      facility_dimension_eom.r_level,
      facility_dimension_eom.division_name,
      facility_dimension_eom.d_level,
      facility_dimension_eom.market_name,
      facility_dimension_eom.f_level,
      facility_dimension_eom.cons_facility_name,
      facility_dimension_eom.lob_code,
      facility_dimension_eom.lob_name,
      facility_dimension_eom.sub_line_of_business_code,
      facility_dimension_eom.sub_line_of_business_name,
      facility_dimension_eom.state_code,
      facility_dimension_eom.pas_id_current,
      facility_dimension_eom.pas_current_name,
      facility_dimension_eom.pas_id_future,
      facility_dimension_eom.pas_future_name,
      facility_dimension_eom.summary_7_member_ind,
      facility_dimension_eom.summary_8_member_ind,
      facility_dimension_eom.summary_phys_svc_member_ind,
      facility_dimension_eom.summary_asd_member_ind,
      facility_dimension_eom.summary_imaging_member_ind,
      facility_dimension_eom.summary_oncology_member_ind,
      facility_dimension_eom.summary_cath_lab_member_ind,
      facility_dimension_eom.summary_intl_member_ind,
      facility_dimension_eom.summary_other_member_ind,
      facility_dimension_eom.pas_coid,
      facility_dimension_eom.pas_status,
      facility_dimension_eom.company_code_operations,
      facility_dimension_eom.osg_pas_ind,
      facility_dimension_eom.abs_facility_member_ind,
      facility_dimension_eom.abl_facility_member_ind,
      facility_dimension_eom.intl_pmis_member_ind,
      facility_dimension_eom.hsc_member_ind,
      facility_dimension_eom.source_system_code,
      facility_dimension_eom.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.facility_dimension_eom
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_aged_ar_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_aged_ar_denial
   OPTIONS(description='Maintains Aged AR of the Accounts  grouping by Patient type, Payor, Denial Code etc.')
  AS SELECT
      fact_aged_ar_denial.coid,
      fact_aged_ar_denial.company_code,
      fact_aged_ar_denial.pe_date,
      fact_aged_ar_denial.unit_num_sid,
      fact_aged_ar_denial.time_id,
      fact_aged_ar_denial.age_month_sid,
      fact_aged_ar_denial.patient_type_sid,
      fact_aged_ar_denial.payor_financial_class_sid,
      fact_aged_ar_denial.payor_sid,
      fact_aged_ar_denial.account_status_sid,
      fact_aged_ar_denial.payor_sequence_sid,
      fact_aged_ar_denial.denial_code_sid,
      fact_aged_ar_denial.rcm_msr_sid,
      ROUND(fact_aged_ar_denial.transaction_amt, 3, 'ROUND_HALF_EVEN') AS transaction_amt,
      fact_aged_ar_denial.source_system_code,
      fact_aged_ar_denial.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_aged_ar_denial
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_ar_patient_level_daily.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_ar_patient_level_daily
   OPTIONS(description='This is Daily  Account Receivable Inventory for HCA and Parallon Customers.')
  AS SELECT
      fact_ar_patient_level_daily.company_code,
      fact_ar_patient_level_daily.coid,
      fact_ar_patient_level_daily.rptg_date,
      fact_ar_patient_level_daily.source_sid,
      fact_ar_patient_level_daily.unit_num_sid,
      ROUND(fact_ar_patient_level_daily.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      fact_ar_patient_level_daily.account_type_sid,
      fact_ar_patient_level_daily.account_status_sid,
      fact_ar_patient_level_daily.patient_type_sid,
      fact_ar_patient_level_daily.age_month_sid,
      fact_ar_patient_level_daily.patient_financial_class_sid,
      fact_ar_patient_level_daily.collection_agency_sid,
      fact_ar_patient_level_daily.payor_financial_class_sid,
      fact_ar_patient_level_daily.product_sid,
      fact_ar_patient_level_daily.contract_sid,
      fact_ar_patient_level_daily.scenario_sid,
      fact_ar_patient_level_daily.payor_sid,
      fact_ar_patient_level_daily.iplan_insurance_order_num,
      fact_ar_patient_level_daily.payor_sequence_sid,
      fact_ar_patient_level_daily.dollar_strf_sid,
      fact_ar_patient_level_daily.denial_sid,
      fact_ar_patient_level_daily.appeal_code_sid,
      fact_ar_patient_level_daily.denial_date,
      fact_ar_patient_level_daily.denial_status_code,
      fact_ar_patient_level_daily.liability_account_cnt,
      fact_ar_patient_level_daily.patient_account_cnt,
      fact_ar_patient_level_daily.discharge_cnt,
      ROUND(fact_ar_patient_level_daily.ar_patient_amt, 3, 'ROUND_HALF_EVEN') AS ar_patient_amt,
      ROUND(fact_ar_patient_level_daily.ar_insurance_amt, 3, 'ROUND_HALF_EVEN') AS ar_insurance_amt,
      ROUND(fact_ar_patient_level_daily.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
      ROUND(fact_ar_patient_level_daily.total_collect_amt, 3, 'ROUND_HALF_EVEN') AS total_collect_amt,
      fact_ar_patient_level_daily.billed_patient_cnt,
      fact_ar_patient_level_daily.discharge_to_billing_day_cnt,
      ROUND(fact_ar_patient_level_daily.gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS gross_charge_amt,
      ROUND(fact_ar_patient_level_daily.prorated_liability_sys_adj_amt, 3, 'ROUND_HALF_EVEN') AS prorated_liability_sys_adj_amt,
      ROUND(fact_ar_patient_level_daily.late_charge_credit_amt, 3, 'ROUND_HALF_EVEN') AS late_charge_credit_amt,
      ROUND(fact_ar_patient_level_daily.late_charge_debit_amt, 3, 'ROUND_HALF_EVEN') AS late_charge_debit_amt,
      ROUND(fact_ar_patient_level_daily.payor_prorated_liability_amt, 3, 'ROUND_HALF_EVEN') AS payor_prorated_liability_amt,
      ROUND(fact_ar_patient_level_daily.payor_payment_amt, 3, 'ROUND_HALF_EVEN') AS payor_payment_amt,
      ROUND(fact_ar_patient_level_daily.payor_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS payor_adjustment_amt,
      ROUND(fact_ar_patient_level_daily.payor_contractual_amt, 3, 'ROUND_HALF_EVEN') AS payor_contractual_amt,
      ROUND(fact_ar_patient_level_daily.payor_denial_amt, 3, 'ROUND_HALF_EVEN') AS payor_denial_amt,
      fact_ar_patient_level_daily.payor_denial_cnt,
      ROUND(fact_ar_patient_level_daily.payor_expected_payment_amt, 3, 'ROUND_HALF_EVEN') AS payor_expected_payment_amt,
      ROUND(fact_ar_patient_level_daily.payor_discrepancy_ovr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS payor_discrepancy_ovr_pmt_amt,
      ROUND(fact_ar_patient_level_daily.payor_discrepancy_undr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS payor_discrepancy_undr_pmt_amt,
      ROUND(fact_ar_patient_level_daily.payor_up_front_collection_amt, 3, 'ROUND_HALF_EVEN') AS payor_up_front_collection_amt,
      fact_ar_patient_level_daily.payor_rebill_cnt,
      fact_ar_patient_level_daily.payor_bill_cnt,
      ROUND(fact_ar_patient_level_daily.unbilled_gross_med_rec_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_gross_med_rec_amt,
      ROUND(fact_ar_patient_level_daily.unbilled_gross_bus_ofc_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_gross_bus_ofc_amt,
      ROUND(fact_ar_patient_level_daily.copay_deduct_amt, 3, 'ROUND_HALF_EVEN') AS copay_deduct_amt,
      fact_ar_patient_level_daily.source_system_code,
      fact_ar_patient_level_daily.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_ar_patient_level_daily
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_dmr_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_dmr_metric
   OPTIONS(description='This table measures many Daily Management report metrics used in Revenue Cycle Project.')
  AS SELECT
      fact_dmr_metric.service_type_name,
      fact_dmr_metric.fact_lvl_code,
      fact_dmr_metric.parent_code,
      fact_dmr_metric.child_code,
      fact_dmr_metric.rptg_date,
      fact_dmr_metric.dmr_day_month_ind,
      fact_dmr_metric.dmr_code,
      fact_dmr_metric.dmr_metric_code,
      fact_dmr_metric.dmr_patient_type_code,
      fact_dmr_metric.dmr_fin_class_code,
      fact_dmr_metric.sub_unit_num,
      ROUND(fact_dmr_metric.dmr_metric_value, 3, 'ROUND_HALF_EVEN') AS dmr_metric_value,
      fact_dmr_metric.source_system_code,
      fact_dmr_metric.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_dmr_metric
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_hsc_delinquency_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_hsc_delinquency_rate
   OPTIONS(description='Health Information Management, Service Center Delinquency Rates rolled up to a facility and a month within a given year.')
  AS SELECT
      fact_hsc_delinquency_rate.company_code,
      fact_hsc_delinquency_rate.coid,
      fact_hsc_delinquency_rate.month_id,
      fact_hsc_delinquency_rate.patient_type_code_pos1,
      fact_hsc_delinquency_rate.unit_num,
      fact_hsc_delinquency_rate.charts_not_revw_thty_day_cnt,
      fact_hsc_delinquency_rate.total_discharge_cnt,
      fact_hsc_delinquency_rate.source_system_code,
      fact_hsc_delinquency_rate.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_hsc_delinquency_rate
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_patient_fin_class_conv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_patient_fin_class_conv
   OPTIONS(description='This table captures tracks the financial class conversions on the patient account from initial registration to service/bill date within a monthly reporting period.')
  AS SELECT
      fact_patient_fin_class_conv.company_code,
      fact_patient_fin_class_conv.coid,
      ROUND(fact_patient_fin_class_conv.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      fact_patient_fin_class_conv.month_id,
      fact_patient_fin_class_conv.unit_num,
      ROUND(fact_patient_fin_class_conv.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      fact_patient_fin_class_conv.patient_type_code,
      fact_patient_fin_class_conv.initial_financial_class_code,
      fact_patient_fin_class_conv.final_financial_class_code,
      fact_patient_fin_class_conv.financial_class_code_initial_date,
      fact_patient_fin_class_conv.financial_class_code_final_date,
      ROUND(fact_patient_fin_class_conv.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      fact_patient_fin_class_conv.source_system_code,
      fact_patient_fin_class_conv.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_patient_fin_class_conv
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_patient_finc_class_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_patient_finc_class_eom
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
      `hca-hin-curated-mirroring-td`.edwpbs.fact_patient_finc_class_eom
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_patient_iplan_conv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_patient_iplan_conv
   OPTIONS(description='This table can be used to determine the conversions in the Payments to Charges Rate by Iplan')
  AS SELECT
      ROUND(fact_patient_iplan_conv.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      fact_patient_iplan_conv.month_id,
      fact_patient_iplan_conv.pe_date,
      fact_patient_iplan_conv.week_month_code,
      fact_patient_iplan_conv.coid,
      fact_patient_iplan_conv.company_code,
      fact_patient_iplan_conv.unit_num,
      ROUND(fact_patient_iplan_conv.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      fact_patient_iplan_conv.same_store_sid,
      fact_patient_iplan_conv.from_patient_type_code,
      fact_patient_iplan_conv.to_patient_type_code,
      fact_patient_iplan_conv.from_financial_class_code,
      fact_patient_iplan_conv.to_financial_class_code,
      fact_patient_iplan_conv.from_iplan_id,
      fact_patient_iplan_conv.to_iplan_id,
      fact_patient_iplan_conv.from_case_cnt,
      fact_patient_iplan_conv.to_case_cnt,
      ROUND(fact_patient_iplan_conv.from_calc_amt, 3, 'ROUND_HALF_EVEN') AS from_calc_amt,
      ROUND(fact_patient_iplan_conv.to_calc_amt, 3, 'ROUND_HALF_EVEN') AS to_calc_amt,
      ROUND(fact_patient_iplan_conv.conv_change_amt, 3, 'ROUND_HALF_EVEN') AS conv_change_amt,
      ROUND(fact_patient_iplan_conv.from_total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS from_total_billed_charge_amt,
      ROUND(fact_patient_iplan_conv.to_total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS to_total_billed_charge_amt,
      ROUND(fact_patient_iplan_conv.from_eor_gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS from_eor_gross_reimbursement_amt,
      ROUND(fact_patient_iplan_conv.to_eor_gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS to_eor_gross_reimbursement_amt,
      ROUND(fact_patient_iplan_conv.from_financial_class_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS from_financial_class_payment_rate_calc,
      ROUND(fact_patient_iplan_conv.to_financial_class_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS to_financial_class_payment_rate_calc,
      ROUND(fact_patient_iplan_conv.from_iplan_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS from_iplan_payment_rate_calc,
      ROUND(fact_patient_iplan_conv.to_iplan_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS to_iplan_payment_rate_calc,
      ROUND(fact_patient_iplan_conv.from_sma_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS from_sma_payment_rate_calc,
      ROUND(fact_patient_iplan_conv.to_sma_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS to_sma_payment_rate_calc,
      fact_patient_iplan_conv.source_system_code,
      fact_patient_iplan_conv.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_patient_iplan_conv
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_ar_hospital_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_rcom_ar_hospital_level AS SELECT
    a.unit_num_sid,
    a.scenario_sid,
    a.week_of_month_sid,
    a.date_sid,
    a.source_sid,
    a.company_code,
    a.coid,
    ROUND(a.recoveries_wo_amt_cons65200, 3, 'ROUND_HALF_EVEN') AS recoveries_wo_amt_cons65200,
    ROUND(a.credit_coll_svc_amt_cons83320, 3, 'ROUND_HALF_EVEN') AS credit_coll_svc_amt_cons83320,
    ROUND(a.allow_govt_receivable_fs05300, 3, 'ROUND_HALF_EVEN') AS allow_govt_receivable_fs05300,
    ROUND(a.allow_uncoll_amt_fs05350, 3, 'ROUND_HALF_EVEN') AS allow_uncoll_amt_fs05350,
    ROUND(a.allow_uncoll_nonpat_amtfs05360, 3, 'ROUND_HALF_EVEN') AS allow_uncoll_nonpat_amtfs05360,
    ROUND(a.ip_revenue_routine_amt_fs50100, 3, 'ROUND_HALF_EVEN') AS ip_revenue_routine_amt_fs50100,
    ROUND(a.ip_rev_ancillary_amt_fs50200, 3, 'ROUND_HALF_EVEN') AS ip_rev_ancillary_amt_fs50200,
    ROUND(a.op_ancillary_rev_amt_fs50400, 3, 'ROUND_HALF_EVEN') AS op_ancillary_rev_amt_fs50400,
    ROUND(a.other_operating_income_fs50900, 3, 'ROUND_HALF_EVEN') AS other_operating_income_fs50900,
    ROUND(a.mcare_cy_cont_ip_amt_fs60100, 3, 'ROUND_HALF_EVEN') AS mcare_cy_cont_ip_amt_fs60100,
    ROUND(a.mcare_cy_cont_op_amt_fs60125, 3, 'ROUND_HALF_EVEN') AS mcare_cy_cont_op_amt_fs60125,
    ROUND(a.prior_yr_cont_ip_amt_fs60150, 3, 'ROUND_HALF_EVEN') AS prior_yr_cont_ip_amt_fs60150,
    ROUND(a.prior_yr_cont_op_amt_fs60175, 3, 'ROUND_HALF_EVEN') AS prior_yr_cont_op_amt_fs60175,
    ROUND(a.mcaid_cy_cont_ip_amt_fs60200, 3, 'ROUND_HALF_EVEN') AS mcaid_cy_cont_ip_amt_fs60200,
    ROUND(a.mcaid_cy_cont_op_amt_fs60225, 3, 'ROUND_HALF_EVEN') AS mcaid_cy_cont_op_amt_fs60225,
    ROUND(a.champ_cy_cont_ip_amt_fs60300, 3, 'ROUND_HALF_EVEN') AS champ_cy_cont_ip_amt_fs60300,
    ROUND(a.champ_cy_cont_op_amt_fs60325, 3, 'ROUND_HALF_EVEN') AS champ_cy_cont_op_amt_fs60325,
    ROUND(a.bc_hmo_ppo_disc_ip_amt_fs60400, 3, 'ROUND_HALF_EVEN') AS bc_hmo_ppo_disc_ip_amt_fs60400,
    ROUND(a.bc_hmo_ppo_disc_op_amt_fs60425, 3, 'ROUND_HALF_EVEN') AS bc_hmo_ppo_disc_op_amt_fs60425,
    ROUND(a.mcare_mgd_care_ip_amt_fs60450, 3, 'ROUND_HALF_EVEN') AS mcare_mgd_care_ip_amt_fs60450,
    ROUND(a.mcare_mgd_care_op_amt_fs60460, 3, 'ROUND_HALF_EVEN') AS mcare_mgd_care_op_amt_fs60460,
    ROUND(a.mcaid_mgd_care_ip_amt_fs60475, 3, 'ROUND_HALF_EVEN') AS mcaid_mgd_care_ip_amt_fs60475,
    ROUND(a.mcaid_mgd_care_op_amt_fs60480, 3, 'ROUND_HALF_EVEN') AS mcaid_mgd_care_op_amt_fs60480,
    ROUND(a.charity_ip_amt_fs60500, 3, 'ROUND_HALF_EVEN') AS charity_ip_amt_fs60500,
    ROUND(a.charity_op_amt_fs60525, 3, 'ROUND_HALF_EVEN') AS charity_op_amt_fs60525,
    ROUND(a.other_deduction_ip_amt_fs60600, 3, 'ROUND_HALF_EVEN') AS other_deduction_ip_amt_fs60600,
    ROUND(a.other_deduction_op_amt_fs60625, 3, 'ROUND_HALF_EVEN') AS other_deduction_op_amt_fs60625,
    ROUND(a.salaries_fdept_620_amt_fs65050, 3, 'ROUND_HALF_EVEN') AS salaries_fdept_620_amt_fs65050,
    ROUND(a.emp_ben_fdept_620_amt_fs65100, 3, 'ROUND_HALF_EVEN') AS emp_ben_fdept_620_amt_fs65100,
    ROUND(a.bad_debt_amt_fs65500, 3, 'ROUND_HALF_EVEN') AS bad_debt_amt_fs65500,
    ROUND(a.patient_receivable_amt_fs90000, 3, 'ROUND_HALF_EVEN') AS patient_receivable_amt_fs90000,
    ROUND(a.reversal_for_05350_amt_fs90008, 3, 'ROUND_HALF_EVEN') AS reversal_for_05350_amt_fs90008,
    ROUND(a.total_patient_rev_amt_fs90230, 3, 'ROUND_HALF_EVEN') AS total_patient_rev_amt_fs90230,
    ROUND(a.curr_year_cont_amt_fs90245, 3, 'ROUND_HALF_EVEN') AS curr_year_cont_amt_fs90245,
    ROUND(a.policy_adjustments_amt_fs90246, 3, 'ROUND_HALF_EVEN') AS policy_adjustments_amt_fs90246,
    ROUND(a.tot_ar_cost_fdept620_gla501000, 3, 'ROUND_HALF_EVEN') AS tot_ar_cost_fdept620_gla501000,
    ROUND(a.mcare_clrng_gla110101_110179, 3, 'ROUND_HALF_EVEN') AS mcare_clrng_gla110101_110179,
    ROUND(a.mcare_clrng_gla110189_110197, 3, 'ROUND_HALF_EVEN') AS mcare_clrng_gla110189_110197,
    ROUND(a.mcaid_clrng_gla110201_110279, 3, 'ROUND_HALF_EVEN') AS mcaid_clrng_gla110201_110279,
    ROUND(a.mcaid_clrng_gla110286_110297, 3, 'ROUND_HALF_EVEN') AS mcaid_clrng_gla110286_110297,
    ROUND(a.cash_receipt_amt, 3, 'ROUND_HALF_EVEN') AS cash_receipt_amt,
    ROUND(a.credit_refund_amt, 3, 'ROUND_HALF_EVEN') AS credit_refund_amt,
    a.reg_accuracy_reg_entry_cnt,
    a.reg_accuracy_reg_changes_cnt,
    ROUND(a.ins_gt_150_day_pct_amt, 5, 'ROUND_HALF_EVEN') AS ins_gt_150_day_pct_amt,
    ROUND(a.ins_gt_90_day_pct_amt, 5, 'ROUND_HALF_EVEN') AS ins_gt_90_day_pct_amt,
    ROUND(a.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
    ROUND(a.unins_ip_gla508490_gla508499, 3, 'ROUND_HALF_EVEN') AS unins_ip_gla508490_gla508499,
    ROUND(a.unins_op_gla508990_gla508999, 3, 'ROUND_HALF_EVEN') AS unins_op_gla508990_gla508999,
    ROUND(a.health_exchange_ip_amt_fs60490, 3, 'ROUND_HALF_EVEN') AS health_exchange_ip_amt_fs60490,
    ROUND(a.health_exchange_op_amt_fs60495, 3, 'ROUND_HALF_EVEN') AS health_exchange_op_amt_fs60495
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.fact_rcom_ar_hospital_level AS a
    INNER JOIN `hca-hin-curated-mirroring-td`.edwpbs.parallon_client_detail AS b ON upper(a.coid) = upper(b.coid)
     AND upper(format_date('%Y%m', date_add(b.go_live_date, interval 0 MONTH))) <= upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_ar_pat_fc_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_rcom_ar_pat_fc_level
   OPTIONS(description='Maintains Gross Revenue and Type 1 Cash AR by Patient financial Class level.')
  AS SELECT
      fact_rcom_ar_pat_fc_level.unit_num_sid,
      fact_rcom_ar_pat_fc_level.patient_financial_class_sid,
      fact_rcom_ar_pat_fc_level.patient_type_sid,
      fact_rcom_ar_pat_fc_level.scenario_sid,
      fact_rcom_ar_pat_fc_level.time_sid,
      fact_rcom_ar_pat_fc_level.source_sid,
      fact_rcom_ar_pat_fc_level.company_code,
      fact_rcom_ar_pat_fc_level.coid,
      ROUND(fact_rcom_ar_pat_fc_level.cash_receipt_amt, 3, 'ROUND_HALF_EVEN') AS cash_receipt_amt,
      ROUND(fact_rcom_ar_pat_fc_level.gross_revenue_amt, 3, 'ROUND_HALF_EVEN') AS gross_revenue_amt,
      fact_rcom_ar_pat_fc_level.source_system_code,
      fact_rcom_ar_pat_fc_level.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_rcom_ar_pat_fc_level
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_ar_patient_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_rcom_ar_patient_level
   OPTIONS(description='Similar to existing PF table that maintains the AR.')
  AS SELECT
      ROUND(fact_rcom_ar_patient_level.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      ROUND(fact_rcom_ar_patient_level.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      fact_rcom_ar_patient_level.account_type_sid,
      fact_rcom_ar_patient_level.account_status_sid,
      fact_rcom_ar_patient_level.age_month_sid,
      fact_rcom_ar_patient_level.patient_financial_class_sid,
      fact_rcom_ar_patient_level.patient_type_sid,
      fact_rcom_ar_patient_level.collection_agency_sid,
      fact_rcom_ar_patient_level.payor_financial_class_sid,
      fact_rcom_ar_patient_level.product_sid,
      fact_rcom_ar_patient_level.contract_sid,
      fact_rcom_ar_patient_level.scenario_sid,
      fact_rcom_ar_patient_level.unit_num_sid,
      fact_rcom_ar_patient_level.source_sid,
      fact_rcom_ar_patient_level.date_sid,
      fact_rcom_ar_patient_level.payor_sid,
      fact_rcom_ar_patient_level.dollar_strf_sid,
      fact_rcom_ar_patient_level.same_store_sid,
      fact_rcom_ar_patient_level.iplan_id,
      fact_rcom_ar_patient_level.iplan_insurance_order_num,
      fact_rcom_ar_patient_level.coid,
      fact_rcom_ar_patient_level.company_code,
      fact_rcom_ar_patient_level.denial_sid,
      fact_rcom_ar_patient_level.appeal_code_sid,
      fact_rcom_ar_patient_level.denial_date,
      fact_rcom_ar_patient_level.denial_status_code,
      fact_rcom_ar_patient_level.patient_account_cnt,
      fact_rcom_ar_patient_level.liability_account_cnt,
      fact_rcom_ar_patient_level.payor_sequence_sid,
      fact_rcom_ar_patient_level.discharge_cnt,
      ROUND(fact_rcom_ar_patient_level.ar_patient_amt, 3, 'ROUND_HALF_EVEN') AS ar_patient_amt,
      ROUND(fact_rcom_ar_patient_level.ar_insurance_amt, 3, 'ROUND_HALF_EVEN') AS ar_insurance_amt,
      ROUND(fact_rcom_ar_patient_level.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
      ROUND(fact_rcom_ar_patient_level.total_collect_amt, 3, 'ROUND_HALF_EVEN') AS total_collect_amt,
      fact_rcom_ar_patient_level.billed_patient_cnt,
      fact_rcom_ar_patient_level.discharge_to_billing_day_cnt,
      ROUND(fact_rcom_ar_patient_level.gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS gross_charge_amt,
      ROUND(fact_rcom_ar_patient_level.late_charge_credit_amt, 3, 'ROUND_HALF_EVEN') AS late_charge_credit_amt,
      ROUND(fact_rcom_ar_patient_level.late_charge_debit_amt, 3, 'ROUND_HALF_EVEN') AS late_charge_debit_amt,
      ROUND(fact_rcom_ar_patient_level.payor_prorated_liability_amt, 3, 'ROUND_HALF_EVEN') AS payor_prorated_liability_amt,
      ROUND(fact_rcom_ar_patient_level.payor_payment_amt, 3, 'ROUND_HALF_EVEN') AS payor_payment_amt,
      ROUND(fact_rcom_ar_patient_level.prorated_liability_sys_adj_amt, 3, 'ROUND_HALF_EVEN') AS prorated_liability_sys_adj_amt,
      ROUND(fact_rcom_ar_patient_level.payor_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS payor_adjustment_amt,
      ROUND(fact_rcom_ar_patient_level.payor_contractual_amt, 3, 'ROUND_HALF_EVEN') AS payor_contractual_amt,
      ROUND(fact_rcom_ar_patient_level.payor_denial_amt, 3, 'ROUND_HALF_EVEN') AS payor_denial_amt,
      fact_rcom_ar_patient_level.payor_denial_cnt,
      ROUND(fact_rcom_ar_patient_level.payor_expected_payment_amt, 3, 'ROUND_HALF_EVEN') AS payor_expected_payment_amt,
      ROUND(fact_rcom_ar_patient_level.payor_discrepancy_ovr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS payor_discrepancy_ovr_pmt_amt,
      ROUND(fact_rcom_ar_patient_level.payor_discrepancy_undr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS payor_discrepancy_undr_pmt_amt,
      ROUND(fact_rcom_ar_patient_level.payor_up_front_collection_amt, 3, 'ROUND_HALF_EVEN') AS payor_up_front_collection_amt,
      fact_rcom_ar_patient_level.payor_bill_cnt,
      fact_rcom_ar_patient_level.payor_rebill_cnt,
      ROUND(fact_rcom_ar_patient_level.unbilled_gross_bus_ofc_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_gross_bus_ofc_amt,
      ROUND(fact_rcom_ar_patient_level.unbilled_gross_med_rec_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_gross_med_rec_amt
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_rcom_ar_patient_level
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_credit_balance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_rcom_pars_credit_balance
   OPTIONS(description='This table contains Credit Balance related details of all patient accounts')
  AS SELECT
      ROUND(fact_rcom_pars_credit_balance.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      fact_rcom_pars_credit_balance.unit_num_sid,
      fact_rcom_pars_credit_balance.account_status_sid,
      fact_rcom_pars_credit_balance.patient_financial_class_sid,
      fact_rcom_pars_credit_balance.patient_type_sid,
      fact_rcom_pars_credit_balance.payor_sid,
      fact_rcom_pars_credit_balance.payor_sequence_sid,
      fact_rcom_pars_credit_balance.credit_status_sid,
      fact_rcom_pars_credit_balance.cr_bal_orig_age_month_sid,
      fact_rcom_pars_credit_balance.date_sid,
      ROUND(fact_rcom_pars_credit_balance.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      fact_rcom_pars_credit_balance.company_code,
      fact_rcom_pars_credit_balance.coid,
      ROUND(fact_rcom_pars_credit_balance.credit_balance_amt, 3, 'ROUND_HALF_EVEN') AS credit_balance_amt,
      fact_rcom_pars_credit_balance.credit_balance_count,
      fact_rcom_pars_credit_balance.discharge_date,
      fact_rcom_pars_credit_balance.credit_balance_date,
      fact_rcom_pars_credit_balance.created_date,
      fact_rcom_pars_credit_balance.source_system_code,
      fact_rcom_pars_credit_balance.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_rcom_pars_credit_balance
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_credit_refund.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_rcom_pars_credit_refund
   OPTIONS(description='This table contains Credit Refunded patient accounts showing Refund amounts and other amounts.')
  AS SELECT
      ROUND(fact_rcom_pars_credit_refund.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      fact_rcom_pars_credit_refund.unit_num_sid,
      fact_rcom_pars_credit_refund.account_status_sid,
      fact_rcom_pars_credit_refund.payor_financial_class_sid,
      fact_rcom_pars_credit_refund.patient_type_sid,
      fact_rcom_pars_credit_refund.payor_sid,
      fact_rcom_pars_credit_refund.refund_type_sid,
      fact_rcom_pars_credit_refund.cr_refund_age_month_sid,
      fact_rcom_pars_credit_refund.date_sid,
      fact_rcom_pars_credit_refund.credit_refund_id,
      ROUND(fact_rcom_pars_credit_refund.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      fact_rcom_pars_credit_refund.company_code,
      fact_rcom_pars_credit_refund.coid,
      fact_rcom_pars_credit_refund.age_of_refund,
      fact_rcom_pars_credit_refund.take_back_ind,
      ROUND(fact_rcom_pars_credit_refund.refund_amount, 3, 'ROUND_HALF_EVEN') AS refund_amount,
      fact_rcom_pars_credit_refund.refund_count,
      ROUND(fact_rcom_pars_credit_refund.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(fact_rcom_pars_credit_refund.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(fact_rcom_pars_credit_refund.total_cash_pay_amt, 3, 'ROUND_HALF_EVEN') AS total_cash_pay_amt,
      ROUND(fact_rcom_pars_credit_refund.total_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_allow_amt,
      ROUND(fact_rcom_pars_credit_refund.total_policy_adj_amt, 3, 'ROUND_HALF_EVEN') AS total_policy_adj_amt,
      ROUND(fact_rcom_pars_credit_refund.total_write_off_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_amt,
      ROUND(fact_rcom_pars_credit_refund.take_back_amt, 3, 'ROUND_HALF_EVEN') AS take_back_amt,
      fact_rcom_pars_credit_refund.discharge_date,
      fact_rcom_pars_credit_refund.credit_balance_date,
      fact_rcom_pars_credit_refund.resolved_date,
      fact_rcom_pars_credit_refund.source_system_code,
      fact_rcom_pars_credit_refund.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_rcom_pars_credit_refund
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_dcrp_all.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_rcom_pars_dcrp_all
   OPTIONS(description='This is Monthly Discrepancy Inventory for all Customers for all Cost year hierarchies')
  AS SELECT
      fact_rcom_pars_dcrp_all.date_sid,
      ROUND(fact_rcom_pars_dcrp_all.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      ROUND(fact_rcom_pars_dcrp_all.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      fact_rcom_pars_dcrp_all.iplan_insurance_order_num,
      fact_rcom_pars_dcrp_all.log_id,
      fact_rcom_pars_dcrp_all.ar_bill_thru_date,
      fact_rcom_pars_dcrp_all.discrepancy_creation_date,
      ROUND(fact_rcom_pars_dcrp_all.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      fact_rcom_pars_dcrp_all.payor_sequence_sid,
      fact_rcom_pars_dcrp_all.unit_num_sid,
      fact_rcom_pars_dcrp_all.eor_log_date,
      fact_rcom_pars_dcrp_all.log_sequence_num,
      fact_rcom_pars_dcrp_all.year_created_sid,
      fact_rcom_pars_dcrp_all.cost_year_sid,
      fact_rcom_pars_dcrp_all.cost_year_end_date,
      fact_rcom_pars_dcrp_all.payor_financial_class_sid,
      fact_rcom_pars_dcrp_all.payor_sid,
      fact_rcom_pars_dcrp_all.patient_type_sid,
      fact_rcom_pars_dcrp_all.discrepancy_age_month_sid,
      fact_rcom_pars_dcrp_all.discharge_age_month_sid,
      fact_rcom_pars_dcrp_all.scenario_sid,
      ROUND(fact_rcom_pars_dcrp_all.reason_code_1_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_1_sid,
      ROUND(fact_rcom_pars_dcrp_all.reason_code_2_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_2_sid,
      ROUND(fact_rcom_pars_dcrp_all.reason_code_3_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_3_sid,
      ROUND(fact_rcom_pars_dcrp_all.reason_code_4_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_4_sid,
      fact_rcom_pars_dcrp_all.source_sid,
      ROUND(fact_rcom_pars_dcrp_all.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id,
      fact_rcom_pars_dcrp_all.coid,
      fact_rcom_pars_dcrp_all.company_code,
      fact_rcom_pars_dcrp_all.resolve_reason_code,
      fact_rcom_pars_dcrp_all.discharge_date,
      fact_rcom_pars_dcrp_all.ra_remit_date,
      fact_rcom_pars_dcrp_all.reason_assignment_date_1,
      fact_rcom_pars_dcrp_all.reason_assignment_date_2,
      fact_rcom_pars_dcrp_all.reason_assignment_date_3,
      fact_rcom_pars_dcrp_all.reason_assignment_date_4,
      ROUND(fact_rcom_pars_dcrp_all.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
      fact_rcom_pars_dcrp_all.primary_reason_code_change_cnt,
      fact_rcom_pars_dcrp_all.discrepancy_days,
      fact_rcom_pars_dcrp_all.discrepancy_resolved_date,
      fact_rcom_pars_dcrp_all.discrepany_type_code,
      fact_rcom_pars_dcrp_all.discrepancy_type_sid,
      fact_rcom_pars_dcrp_all.same_store_sid,
      fact_rcom_pars_dcrp_all.dollar_strtf_sid,
      ROUND(fact_rcom_pars_dcrp_all.var_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_beg_amt,
      ROUND(fact_rcom_pars_dcrp_all.var_new_amt, 3, 'ROUND_HALF_EVEN') AS var_new_amt,
      ROUND(fact_rcom_pars_dcrp_all.var_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_resolved_amt,
      ROUND(fact_rcom_pars_dcrp_all.var_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_othr_cor_amt,
      ROUND(fact_rcom_pars_dcrp_all.var_end_amt, 3, 'ROUND_HALF_EVEN') AS var_end_amt,
      ROUND(fact_rcom_pars_dcrp_all.exp_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_crnt_amt,
      ROUND(fact_rcom_pars_dcrp_all.exp_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_rate_amt,
      fact_rcom_pars_dcrp_all.var_beg_cnt,
      fact_rcom_pars_dcrp_all.var_new_cnt,
      fact_rcom_pars_dcrp_all.var_resolved_cnt,
      fact_rcom_pars_dcrp_all.var_othr_cor_cnt,
      fact_rcom_pars_dcrp_all.var_end_cnt,
      fact_rcom_pars_dcrp_all.exp_crnt_cnt,
      fact_rcom_pars_dcrp_all.exp_rate_cnt,
      fact_rcom_pars_dcrp_all.resolved_days,
      fact_rcom_pars_dcrp_all.end_days,
      fact_rcom_pars_dcrp_all.pe_date,
      fact_rcom_pars_dcrp_all.source_system_code,
      fact_rcom_pars_dcrp_all.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_rcom_pars_dcrp_all
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_rcom_pars_denial
   OPTIONS(description='This is Monthly Denial Inventory for HCA and Parallon Customers.')
  AS SELECT
      ROUND(fact_rcom_pars_denial.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      fact_rcom_pars_denial.date_sid,
      fact_rcom_pars_denial.payor_sid,
      fact_rcom_pars_denial.payor_financial_class_sid,
      fact_rcom_pars_denial.payor_sequence_sid,
      fact_rcom_pars_denial.patient_type_sid,
      fact_rcom_pars_denial.appeal_disp_sid,
      fact_rcom_pars_denial.appeal_code_sid,
      fact_rcom_pars_denial.discharge_age_month_sid,
      fact_rcom_pars_denial.denial_orig_age_month_sid,
      fact_rcom_pars_denial.appeal_age_month_sid,
      fact_rcom_pars_denial.service_code_sid,
      fact_rcom_pars_denial.unit_num_sid,
      fact_rcom_pars_denial.los_sid,
      fact_rcom_pars_denial.drg_sid,
      fact_rcom_pars_denial.admission_type_sid,
      fact_rcom_pars_denial.denial_type_sid,
      fact_rcom_pars_denial.appeal_root_cause_sid,
      fact_rcom_pars_denial.scenario_sid,
      fact_rcom_pars_denial.source_sid,
      ROUND(fact_rcom_pars_denial.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      fact_rcom_pars_denial.iplan_id,
      ROUND(fact_rcom_pars_denial.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      fact_rcom_pars_denial.coid,
      fact_rcom_pars_denial.company_code,
      fact_rcom_pars_denial.same_store_sid,
      fact_rcom_pars_denial.resolved_denial_age_month_id,
      fact_rcom_pars_denial.resolved_discharge_age_month_id,
      fact_rcom_pars_denial.dollar_strf_sid,
      fact_rcom_pars_denial.person_role_code,
      fact_rcom_pars_denial.patient_full_name,
      fact_rcom_pars_denial.patient_birth_date,
      ROUND(fact_rcom_pars_denial.beginning_balance_amt, 3, 'ROUND_HALF_EVEN') AS beginning_balance_amt,
      fact_rcom_pars_denial.beginning_balance_count,
      ROUND(fact_rcom_pars_denial.beginning_appeal_amt, 3, 'ROUND_HALF_EVEN') AS beginning_appeal_amt,
      ROUND(fact_rcom_pars_denial.ending_balance_amt, 3, 'ROUND_HALF_EVEN') AS ending_balance_amt,
      fact_rcom_pars_denial.ending_balance_count,
      ROUND(fact_rcom_pars_denial.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
      ROUND(fact_rcom_pars_denial.write_off_denial_account_amt, 3, 'ROUND_HALF_EVEN') AS write_off_denial_account_amt,
      fact_rcom_pars_denial.write_off_denial_account_count,
      ROUND(fact_rcom_pars_denial.new_denial_account_amt, 3, 'ROUND_HALF_EVEN') AS new_denial_account_amt,
      fact_rcom_pars_denial.new_denial_account_count,
      ROUND(fact_rcom_pars_denial.not_true_denial_amt, 3, 'ROUND_HALF_EVEN') AS not_true_denial_amt,
      fact_rcom_pars_denial.not_true_denial_count,
      ROUND(fact_rcom_pars_denial.corrections_account_amt, 3, 'ROUND_HALF_EVEN') AS corrections_account_amt,
      fact_rcom_pars_denial.corrections_account_count,
      ROUND(fact_rcom_pars_denial.overturned_account_amt, 3, 'ROUND_HALF_EVEN') AS overturned_account_amt,
      fact_rcom_pars_denial.overturned_account_count,
      ROUND(fact_rcom_pars_denial.cc_corrections_account_amt, 3, 'ROUND_HALF_EVEN') AS cc_corrections_account_amt,
      fact_rcom_pars_denial.cc_corrections_account_cnt,
      ROUND(fact_rcom_pars_denial.cc_overturned_account_amt, 3, 'ROUND_HALF_EVEN') AS cc_overturned_account_amt,
      fact_rcom_pars_denial.cc_overturned_account_cnt,
      ROUND(fact_rcom_pars_denial.trans_next_party_amt, 3, 'ROUND_HALF_EVEN') AS trans_next_party_amt,
      fact_rcom_pars_denial.trans_next_party_count,
      ROUND(fact_rcom_pars_denial.unworked_conversion_amt, 3, 'ROUND_HALF_EVEN') AS unworked_conversion_amt,
      fact_rcom_pars_denial.unworked_conversion_count,
      fact_rcom_pars_denial.resolved_accounts_count,
      ROUND(fact_rcom_pars_denial.below_threshold_amt, 3, 'ROUND_HALF_EVEN') AS below_threshold_amt,
      fact_rcom_pars_denial.below_threshold_count,
      fact_rcom_pars_denial.appeal_closing_date,
      fact_rcom_pars_denial.appeal_level_origination_date,
      fact_rcom_pars_denial.appeal_deadline_date,
      fact_rcom_pars_denial.appeal_level_num,
      fact_rcom_pars_denial.work_again_date,
      fact_rcom_pars_denial.attending_physician_name,
      ROUND(fact_rcom_pars_denial.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      fact_rcom_pars_denial.last_update_id,
      fact_rcom_pars_denial.last_update_date,
      fact_rcom_pars_denial.appeal_origination_date,
      fact_rcom_pars_denial.discharge_date,
      fact_rcom_pars_denial.resolved_days,
      fact_rcom_pars_denial.open_days,
      fact_rcom_pars_denial.overturned_days,
      fact_rcom_pars_denial.final_bill_count,
      ROUND(fact_rcom_pars_denial.final_bill_charge_amt, 3, 'ROUND_HALF_EVEN') AS final_bill_charge_amt,
      ROUND(fact_rcom_pars_denial.denied_charge_amt, 3, 'ROUND_HALF_EVEN') AS denied_charge_amt,
      ROUND(fact_rcom_pars_denial.cash_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS cash_adjustment_amt,
      ROUND(fact_rcom_pars_denial.contractual_allow_adj_amt, 3, 'ROUND_HALF_EVEN') AS contractual_allow_adj_amt,
      ROUND(fact_rcom_pars_denial.cc_appeal_num, 0, 'ROUND_HALF_EVEN') AS cc_appeal_num,
      fact_rcom_pars_denial.cc_appeal_detail_seq_num,
      ROUND(fact_rcom_pars_denial.cc_appeal_crnt_bal_amt, 3, 'ROUND_HALF_EVEN') AS cc_appeal_crnt_bal_amt,
      fact_rcom_pars_denial.pa_vendor_code,
      fact_rcom_pars_denial.prepay_mc_flag,
      fact_rcom_pars_denial.pe_date,
      fact_rcom_pars_denial.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_rcom_pars_denial
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_discrepancy.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_rcom_pars_discrepancy AS SELECT
    fact_rcom_pars_discrepancy.date_sid,
    ROUND(fact_rcom_pars_discrepancy.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(fact_rcom_pars_discrepancy.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    fact_rcom_pars_discrepancy.iplan_insurance_order_num,
    fact_rcom_pars_discrepancy.eor_log_date,
    fact_rcom_pars_discrepancy.log_id,
    fact_rcom_pars_discrepancy.ar_bill_thru_date,
    fact_rcom_pars_discrepancy.log_sequence_num,
    fact_rcom_pars_discrepancy.discrepancy_creation_date,
    CASE
       CASE
         substr(CAST(fact_rcom_pars_discrepancy.date_sid as STRING), 1, 4)
        WHEN '' THEN 0
        ELSE CAST(substr(CAST(fact_rcom_pars_discrepancy.date_sid as STRING), 1, 4) as INT64)
      END - CASE
         format_date('%Y', fact_rcom_pars_discrepancy.discrepancy_creation_date)
        WHEN '' THEN 0
        ELSE CAST(format_date('%Y', fact_rcom_pars_discrepancy.discrepancy_creation_date) as INT64)
      END
      WHEN 0 THEN 1
      WHEN 1 THEN 2
      WHEN 2 THEN 3
      WHEN 3 THEN 4
      ELSE 4
    END AS year_created_sid,
    ROUND(fact_rcom_pars_discrepancy.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
    fact_rcom_pars_discrepancy.payor_financial_class_sid,
    fact_rcom_pars_discrepancy.payor_sid,
    fact_rcom_pars_discrepancy.payor_sequence_sid,
    fact_rcom_pars_discrepancy.patient_type_sid,
    fact_rcom_pars_discrepancy.unit_num_sid,
    fact_rcom_pars_discrepancy.discrepancy_age_month_sid,
    fact_rcom_pars_discrepancy.discharge_age_month_sid,
    fact_rcom_pars_discrepancy.scenario_sid,
    ROUND(fact_rcom_pars_discrepancy.reason_code_1_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_1_sid,
    ROUND(fact_rcom_pars_discrepancy.reason_code_2_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_2_sid,
    ROUND(fact_rcom_pars_discrepancy.reason_code_3_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_3_sid,
    ROUND(fact_rcom_pars_discrepancy.reason_code_4_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_4_sid,
    fact_rcom_pars_discrepancy.gross_rbmt_dcrp_type_sid,
    fact_rcom_pars_discrepancy.cont_alw_dcrp_type_sid,
    fact_rcom_pars_discrepancy.payment_dcrp_type_sid,
    fact_rcom_pars_discrepancy.charge_dcrp_type_sid,
    fact_rcom_pars_discrepancy.gross_rbmt_new_strf_sid,
    fact_rcom_pars_discrepancy.cont_alw_new_strf_sid,
    fact_rcom_pars_discrepancy.payment_new_strf_sid,
    fact_rcom_pars_discrepancy.charge_new_strf_sid,
    fact_rcom_pars_discrepancy.gross_rbmt_end_strf_sid,
    fact_rcom_pars_discrepancy.cont_alw_end_strf_sid,
    fact_rcom_pars_discrepancy.payment_end_strf_sid,
    fact_rcom_pars_discrepancy.charge_end_strf_sid,
    fact_rcom_pars_discrepancy.source_sid,
    fact_rcom_pars_discrepancy.coid,
    fact_rcom_pars_discrepancy.company_code,
    fact_rcom_pars_discrepancy.resolve_reason_code,
    fact_rcom_pars_discrepancy.discharge_date,
    fact_rcom_pars_discrepancy.ra_remit_date,
    fact_rcom_pars_discrepancy.reason_assignment_date_1,
    fact_rcom_pars_discrepancy.reason_assignment_date_2,
    fact_rcom_pars_discrepancy.reason_assignment_date_3,
    fact_rcom_pars_discrepancy.reason_assignment_date_4,
    ROUND(fact_rcom_pars_discrepancy.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
    ROUND(fact_rcom_pars_discrepancy.var_gross_rbmt_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_rbmt_beg_amt,
    ROUND(fact_rcom_pars_discrepancy.var_gross_rbmt_new_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_rbmt_new_amt,
    ROUND(fact_rcom_pars_discrepancy.var_gross_rbmt_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_rbmt_resolved_amt,
    ROUND(fact_rcom_pars_discrepancy.var_gross_rbmt_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_rbmt_othr_cor_amt,
    ROUND(fact_rcom_pars_discrepancy.var_gross_rbmt_end_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_rbmt_end_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_gross_rbmt_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_gross_rbmt_crnt_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_gross_rbmt_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_gross_rbmt_rate_amt,
    fact_rcom_pars_discrepancy.var_gross_rbmt_beg_cnt,
    fact_rcom_pars_discrepancy.var_gross_rbmt_new_cnt,
    fact_rcom_pars_discrepancy.var_gross_rbmt_resolved_cnt,
    fact_rcom_pars_discrepancy.var_gross_rbmt_othr_cor_cnt,
    fact_rcom_pars_discrepancy.var_gross_rbmt_end_cnt,
    fact_rcom_pars_discrepancy.exp_gross_rbmt_crnt_cnt,
    fact_rcom_pars_discrepancy.exp_gross_rbmt_rate_cnt,
    ROUND(fact_rcom_pars_discrepancy.var_cont_alw_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_cont_alw_beg_amt,
    ROUND(fact_rcom_pars_discrepancy.var_cont_alw_new_amt, 3, 'ROUND_HALF_EVEN') AS var_cont_alw_new_amt,
    ROUND(fact_rcom_pars_discrepancy.var_cont_alw_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_cont_alw_resolved_amt,
    ROUND(fact_rcom_pars_discrepancy.var_cont_alw_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_cont_alw_othr_cor_amt,
    ROUND(fact_rcom_pars_discrepancy.var_cont_alw_end_amt, 3, 'ROUND_HALF_EVEN') AS var_cont_alw_end_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_cont_alw_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_cont_alw_crnt_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_cont_alw_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_cont_alw_rate_amt,
    fact_rcom_pars_discrepancy.var_cont_alw_beg_cnt,
    fact_rcom_pars_discrepancy.var_cont_alw_new_cnt,
    fact_rcom_pars_discrepancy.var_cont_alw_resolved_cnt,
    fact_rcom_pars_discrepancy.var_cont_alw_othr_cor_cnt,
    fact_rcom_pars_discrepancy.var_cont_alw_end_cnt,
    fact_rcom_pars_discrepancy.exp_cont_alw_crnt_cnt,
    fact_rcom_pars_discrepancy.exp_cont_alw_rate_cnt,
    ROUND(fact_rcom_pars_discrepancy.var_payment_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_payment_beg_amt,
    ROUND(fact_rcom_pars_discrepancy.var_payment_new_amt, 3, 'ROUND_HALF_EVEN') AS var_payment_new_amt,
    ROUND(fact_rcom_pars_discrepancy.var_payment_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_payment_resolved_amt,
    ROUND(fact_rcom_pars_discrepancy.var_payment_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_payment_othr_cor_amt,
    ROUND(fact_rcom_pars_discrepancy.var_payment_end_amt, 3, 'ROUND_HALF_EVEN') AS var_payment_end_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_payment_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_payment_crnt_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_payment_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_payment_rate_amt,
    fact_rcom_pars_discrepancy.var_payment_beg_cnt,
    fact_rcom_pars_discrepancy.var_payment_new_cnt,
    fact_rcom_pars_discrepancy.var_payment_resolved_cnt,
    fact_rcom_pars_discrepancy.var_payment_othr_cor_cnt,
    fact_rcom_pars_discrepancy.var_payment_end_cnt,
    fact_rcom_pars_discrepancy.exp_payment_crnt_cnt,
    fact_rcom_pars_discrepancy.exp_payment_rate_cnt,
    ROUND(fact_rcom_pars_discrepancy.var_charge_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_charge_beg_amt,
    ROUND(fact_rcom_pars_discrepancy.var_charge_new_amt, 3, 'ROUND_HALF_EVEN') AS var_charge_new_amt,
    ROUND(fact_rcom_pars_discrepancy.var_charge_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_charge_resolved_amt,
    ROUND(fact_rcom_pars_discrepancy.var_charge_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_charge_othr_cor_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_charge_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_charge_crnt_amt,
    ROUND(fact_rcom_pars_discrepancy.var_charge_end_amt, 3, 'ROUND_HALF_EVEN') AS var_charge_end_amt,
    fact_rcom_pars_discrepancy.var_charge_beg_cnt,
    fact_rcom_pars_discrepancy.var_charge_new_cnt,
    fact_rcom_pars_discrepancy.var_charge_resolved_cnt,
    fact_rcom_pars_discrepancy.var_charge_othr_cor_cnt,
    fact_rcom_pars_discrepancy.exp_charge_crnt_cnt,
    fact_rcom_pars_discrepancy.var_charge_end_cnt,
    fact_rcom_pars_discrepancy.primary_reason_code_change_cnt,
    fact_rcom_pars_discrepancy.discrepancy_days,
    fact_rcom_pars_discrepancy.discrepancy_resolved_date,
    ROUND(fact_rcom_pars_discrepancy.cc_reason_id, 0, 'ROUND_HALF_EVEN') AS cc_reason_id,
    ROUND(fact_rcom_pars_discrepancy.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.fact_rcom_pars_discrepancy
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_unbilled_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_unbilled_inventory
   OPTIONS(description='Daily Inventory of Unbilled accounts to monitor the billing cycle.')
  AS SELECT
      fact_unbilled_inventory.rptg_date,
      ROUND(fact_unbilled_inventory.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      fact_unbilled_inventory.claim_id,
      fact_unbilled_inventory.request_id,
      fact_unbilled_inventory.queue_dept_id,
      fact_unbilled_inventory.unbilled_status_code,
      fact_unbilled_inventory.unbilled_reason_code,
      fact_unbilled_inventory.him_unbilled_reason_code,
      fact_unbilled_inventory.acct_type_desc,
      fact_unbilled_inventory.request_created_date,
      fact_unbilled_inventory.queue_assigned_date,
      fact_unbilled_inventory.last_activity_date,
      ROUND(fact_unbilled_inventory.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      fact_unbilled_inventory.coid,
      fact_unbilled_inventory.company_code,
      fact_unbilled_inventory.unit_num,
      fact_unbilled_inventory.final_bill_date,
      fact_unbilled_inventory.discharge_date,
      fact_unbilled_inventory.admission_date,
      fact_unbilled_inventory.patient_type_code_pos1,
      fact_unbilled_inventory.payor_sid,
      ROUND(fact_unbilled_inventory.payor_dw_id_ins1, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins1,
      fact_unbilled_inventory.iplan_id_ins1,
      fact_unbilled_inventory.payor_financial_class_sid,
      ROUND(fact_unbilled_inventory.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      ROUND(fact_unbilled_inventory.patient_person_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_person_dw_id,
      ROUND(fact_unbilled_inventory.gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS gross_charge_amt,
      ROUND(fact_unbilled_inventory.alert_gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS alert_gross_charge_amt,
      ROUND(fact_unbilled_inventory.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(fact_unbilled_inventory.alert_total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS alert_total_account_balance_amt,
      ROUND(fact_unbilled_inventory.rh_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS rh_total_charge_amt,
      fact_unbilled_inventory.hold_bill_reason_code,
      fact_unbilled_inventory.account_process_ind,
      fact_unbilled_inventory.unbilled_responsibility_ind,
      fact_unbilled_inventory.claim_submit_date,
      fact_unbilled_inventory.final_bill_hold_ind,
      fact_unbilled_inventory.bill_type_code,
      fact_unbilled_inventory.date_of_claim,
      fact_unbilled_inventory.request_file_id,
      fact_unbilled_inventory.source_system_code,
      fact_unbilled_inventory.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_unbilled_inventory
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/hps_pat_payment_link_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.hps_pat_payment_link_dtl
   OPTIONS(description='This table is used for reporting the Payment link details which are created for the HPS transactions and passed on to PA to settle the patient balances.')
  AS SELECT
      ROUND(hps_pat_payment_link_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      hps_pat_payment_link_dtl.coid,
      hps_pat_payment_link_dtl.payment_link_created_date_time_ct,
      hps_pat_payment_link_dtl.payment_link_accessed_seq_num,
      ROUND(hps_pat_payment_link_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      hps_pat_payment_link_dtl.unit_num,
      hps_pat_payment_link_dtl.company_code,
      hps_pat_payment_link_dtl.adjustment_applied_ind,
      ROUND(hps_pat_payment_link_dtl.payment_amt, 3, 'ROUND_HALF_EVEN') AS payment_amt,
      ROUND(hps_pat_payment_link_dtl.adjustment_amt, 3, 'ROUND_HALF_EVEN') AS adjustment_amt,
      hps_pat_payment_link_dtl.payment_link_last_accessed_date_time_ct,
      hps_pat_payment_link_dtl.payment_processed_ind,
      hps_pat_payment_link_dtl.payment_processed_date_time_ct,
      hps_pat_payment_link_dtl.payment_taker_user_id,
      hps_pat_payment_link_dtl.payment_link_expiration_date_time_ct,
      hps_pat_payment_link_dtl.unsubscribed_reminder_ind,
      hps_pat_payment_link_dtl.unsubscribed_reminder_timestamp,
      hps_pat_payment_link_dtl.email_reminder_sent_timestamp,
      hps_pat_payment_link_dtl.email_reminder_sent_cnt,
      hps_pat_payment_link_dtl.data_source_code,
      hps_pat_payment_link_dtl.source_system_code,
      hps_pat_payment_link_dtl.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.hps_pat_payment_link_dtl
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_appeal_disposition.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.junc_appeal_disposition AS SELECT
    junc_appeal_disposition.disposition_num,
    junc_appeal_disposition.cc_disposition_code,
    junc_appeal_disposition.disposition_desc,
    junc_appeal_disposition.disposition_status,
    junc_appeal_disposition.appeal_disp_sid
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.junc_appeal_disposition
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_remittance_additional_payee.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.junc_remittance_additional_payee
   OPTIONS(description='Crosswalk table for Payment Records and Additional Payee Details')
  AS SELECT
      junc_remittance_additional_payee.payment_guid,
      junc_remittance_additional_payee.additional_payee_line_num,
      junc_remittance_additional_payee.remittance_additional_payee_sid,
      junc_remittance_additional_payee.source_system_code,
      junc_remittance_additional_payee.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.junc_remittance_additional_payee
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_remittance_delete.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.junc_remittance_delete
   OPTIONS(description='Crosswalk table for Payment Records to be deleted.')
  AS SELECT
      junc_remittance_delete.check_num_an,
      junc_remittance_delete.check_date,
      ROUND(junc_remittance_delete.check_amt, 3, 'ROUND_HALF_EVEN') AS check_amt,
      junc_remittance_delete.interchange_sender_id,
      junc_remittance_delete.provider_adjustment_id,
      junc_remittance_delete.payment_guid,
      junc_remittance_delete.claim_guid,
      junc_remittance_delete.service_guid,
      junc_remittance_delete.delete_date,
      junc_remittance_delete.coid,
      junc_remittance_delete.unit_num,
      junc_remittance_delete.company_code,
      junc_remittance_delete.source_system_code,
      junc_remittance_delete.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.junc_remittance_delete
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_remittance_other_claim_related_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.junc_remittance_other_claim_related_info
   OPTIONS(description='Crosswalk table for Claim Records and Other Claim Related Related Information')
  AS SELECT
      junc_remittance_other_claim_related_info.claim_guid,
      junc_remittance_other_claim_related_info.payment_guid,
      ROUND(junc_remittance_other_claim_related_info.reference_id_line_num, 0, 'ROUND_HALF_EVEN') AS reference_id_line_num,
      ROUND(junc_remittance_other_claim_related_info.ref_sid, 0, 'ROUND_HALF_EVEN') AS ref_sid,
      junc_remittance_other_claim_related_info.source_system_code,
      junc_remittance_other_claim_related_info.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.junc_remittance_other_claim_related_info
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_remittance_provider_serv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.junc_remittance_provider_serv
   OPTIONS(description='Crosswalk table for Service Records with Provider Service Details')
  AS SELECT
      junc_remittance_provider_serv.service_guid,
      junc_remittance_provider_serv.provider_serv_id_line_num,
      ROUND(junc_remittance_provider_serv.remittance_provider_serv_sid, 0, 'ROUND_HALF_EVEN') AS remittance_provider_serv_sid,
      junc_remittance_provider_serv.source_system_code,
      junc_remittance_provider_serv.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.junc_remittance_provider_serv
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_remittance_secn_rendering_prov.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.junc_remittance_secn_rendering_prov
   OPTIONS(description='Crosswalk table for Claim Records & Service Records with Secondary Rendering Provider Details')
  AS SELECT
      junc_remittance_secn_rendering_prov.claim_guid,
      junc_remittance_secn_rendering_prov.payment_guid,
      junc_remittance_secn_rendering_prov.service_guid,
      junc_remittance_secn_rendering_prov.secn_rendering_provider_id_line_num,
      ROUND(junc_remittance_secn_rendering_prov.remittance_secn_rendering_provider_sid, 0, 'ROUND_HALF_EVEN') AS remittance_secn_rendering_provider_sid,
      junc_remittance_secn_rendering_prov.source_system_code,
      junc_remittance_secn_rendering_prov.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.junc_remittance_secn_rendering_prov
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_vendor_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.junc_vendor_payor AS SELECT
    junc_vendor_payor.coid,
    junc_vendor_payor.hosp_no,
    junc_vendor_payor.vendor_key,
    junc_vendor_payor.ins_plan,
    junc_vendor_payor.ins_plan_desc,
    junc_vendor_payor.payor_name,
    junc_vendor_payor.major_payor_desc,
    junc_vendor_payor.financial_class_code,
    junc_vendor_payor.dw_last_update_date_time
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.junc_vendor_payor
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_dmr_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.lu_dmr_code AS SELECT
    lu_dmr_code.dmr_code,
    lu_dmr_code.dmr_desc,
    lu_dmr_code.dmr_type_code,
    lu_dmr_code.dw_last_update_date_time,
    lu_dmr_code.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.lu_dmr_code
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_erequest_source_system.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.lu_erequest_source_system
   OPTIONS(description='Reference table to describe Erequest Type')
  AS SELECT
      lu_erequest_source_system.erequest_src_sys_sid,
      lu_erequest_source_system.erequest_src_sys_desc,
      lu_erequest_source_system.erequest_src_sys_category_code,
      lu_erequest_source_system.erequest_src_sys_group_code,
      lu_erequest_source_system.source_system_code,
      lu_erequest_source_system.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.lu_erequest_source_system
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_rcm_level_security.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.lu_rcm_level_security AS SELECT
    lu_rcm_level_security.service_type_name,
    lu_rcm_level_security.fact_lvl_code,
    lu_rcm_level_security.parent_code,
    lu_rcm_level_security.coid,
    lu_rcm_level_security.parent_desc,
    lu_rcm_level_security.dw_last_update_date_time,
    lu_rcm_level_security.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.lu_rcm_level_security
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_same_store.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.lu_same_store
   OPTIONS(description='Lookup table that is loaded monthly to determine if a facility is in Same Store before and after GL Close')
  AS SELECT
      lu_same_store.month_id,
      lu_same_store.company_code,
      lu_same_store.coid,
      lu_same_store.gl_close_ind,
      lu_same_store.same_store_ind,
      lu_same_store.source_system_code,
      lu_same_store.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.lu_same_store
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_year_month_past.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.lu_year_month_past AS SELECT
    lu_year_month_past.month_id,
    lu_year_month_past.month_desc_s,
    lu_year_month_past.prior_month_id,
    lu_year_month_past.prior_2_month_id,
    lu_year_month_past.prior_3_month_id,
    lu_year_month_past.prior_year_month_id,
    lu_year_month_past.prior_2_year_month_id,
    lu_year_month_past.prior_3_year_month_id,
    lu_year_month_past.current_month_flag,
    lu_year_month_past.curent_year,
    lu_year_month_past.prior_year,
    lu_year_month_past.prior_2_year,
    lu_year_month_past.prior_3_year,
    lu_year_month_past.current_year_flag,
    lu_year_month_past.prev_month_days,
    lu_year_month_past.prev_2_month_days,
    lu_year_month_past.prev_3_month_days,
    lu_year_month_past.month_36_ind,
    lu_year_month_past.month_13_ind,
    lu_year_month_past.dw_last_update_date_time,
    lu_year_month_past.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.lu_year_month_past
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/mcaid_elig_placement_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.mcaid_elig_placement_dtl
   OPTIONS(description='Monthly snapshot table to load Medicaid Eligibility Placement Details only')
  AS SELECT
      ROUND(mcaid_elig_placement_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      mcaid_elig_placement_dtl.pe_date,
      mcaid_elig_placement_dtl.coid,
      mcaid_elig_placement_dtl.company_code,
      mcaid_elig_placement_dtl.patient_type_alias_name,
      mcaid_elig_placement_dtl.unit_num,
      ROUND(mcaid_elig_placement_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      mcaid_elig_placement_dtl.mcaid_elig_phase_id,
      mcaid_elig_placement_dtl.mcaid_elig_acct_status_code,
      mcaid_elig_placement_dtl.elig_referral_date,
      mcaid_elig_placement_dtl.application_approved_date,
      mcaid_elig_placement_dtl.closed_date,
      mcaid_elig_placement_dtl.application_filed_date,
      mcaid_elig_placement_dtl.approval_age_num,
      mcaid_elig_placement_dtl.source_system_code,
      mcaid_elig_placement_dtl.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.mcaid_elig_placement_dtl
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/mcaid_eligibility_acct_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.mcaid_eligibility_acct_dtl
   OPTIONS(description='Table will maintain the daily inventory of accounts from Parallon Medicaid Eligibility tool.')
  AS SELECT
      ROUND(mcaid_eligibility_acct_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      mcaid_eligibility_acct_dtl.status_code,
      mcaid_eligibility_acct_dtl.referral_date,
      mcaid_eligibility_acct_dtl.closed_date,
      mcaid_eligibility_acct_dtl.company_code,
      mcaid_eligibility_acct_dtl.coid,
      ROUND(mcaid_eligibility_acct_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      mcaid_eligibility_acct_dtl.elig_phase_code,
      mcaid_eligibility_acct_dtl.program_code,
      mcaid_eligibility_acct_dtl.application_filed_date,
      mcaid_eligibility_acct_dtl.approved_date,
      mcaid_eligibility_acct_dtl.source_system_code,
      mcaid_eligibility_acct_dtl.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.mcaid_eligibility_acct_dtl
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_analysis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.net_revenue_impact_analysis
   OPTIONS(description='This table can be used to determine Monthly/Weekly data such as Credit Refund, Denial, Discrepancy Inventory, Underpayment accounts etc that impacts net revenue.')
  AS SELECT
      ROUND(net_revenue_impact_analysis.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      net_revenue_impact_analysis.coid,
      ROUND(net_revenue_impact_analysis.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      net_revenue_impact_analysis.metric_code,
      net_revenue_impact_analysis.month_id,
      net_revenue_impact_analysis.pe_date,
      net_revenue_impact_analysis.week_month_code,
      ROUND(net_revenue_impact_analysis.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      net_revenue_impact_analysis.company_code,
      net_revenue_impact_analysis.unit_num,
      ROUND(net_revenue_impact_analysis.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      net_revenue_impact_analysis.iplan_id,
      net_revenue_impact_analysis.discharge_date,
      net_revenue_impact_analysis.patient_type_code,
      ROUND(net_revenue_impact_analysis.metric_amt, 3, 'ROUND_HALF_EVEN') AS metric_amt,
      net_revenue_impact_analysis.dw_last_update_date_time,
      net_revenue_impact_analysis.source_system_code
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.net_revenue_impact_analysis
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_credit_refund.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.net_revenue_impact_credit_refund
   OPTIONS(description='Daily Credit Refund data to capture net revenue impact.')
  AS SELECT
      net_revenue_impact_credit_refund.reporting_date,
      ROUND(net_revenue_impact_credit_refund.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      net_revenue_impact_credit_refund.iplan_id,
      net_revenue_impact_credit_refund.company_code,
      net_revenue_impact_credit_refund.coid,
      net_revenue_impact_credit_refund.unit_num,
      ROUND(net_revenue_impact_credit_refund.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(net_revenue_impact_credit_refund.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      net_revenue_impact_credit_refund.admission_date,
      net_revenue_impact_credit_refund.discharge_date,
      net_revenue_impact_credit_refund.entered_date,
      net_revenue_impact_credit_refund.patient_type_code,
      ROUND(net_revenue_impact_credit_refund.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      net_revenue_impact_credit_refund.payment_discrepancy_ind,
      net_revenue_impact_credit_refund.cm_dcrp_rslvd_ind,
      ROUND(net_revenue_impact_credit_refund.refund_amt, 3, 'ROUND_HALF_EVEN') AS refund_amt,
      net_revenue_impact_credit_refund.dw_last_update_date_time,
      net_revenue_impact_credit_refund.source_system_code
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.net_revenue_impact_credit_refund
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.net_revenue_impact_denial
   OPTIONS(description='Daily Denial data to capture net revenue impact.')
  AS SELECT
      net_revenue_impact_denial.reporting_date,
      ROUND(net_revenue_impact_denial.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      net_revenue_impact_denial.iplan_id,
      net_revenue_impact_denial.iplan_insurance_order_num,
      ROUND(net_revenue_impact_denial.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      net_revenue_impact_denial.company_code,
      net_revenue_impact_denial.coid,
      net_revenue_impact_denial.unit_num,
      ROUND(net_revenue_impact_denial.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ROUND(net_revenue_impact_denial.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      net_revenue_impact_denial.denial_status_code,
      net_revenue_impact_denial.patient_type_code,
      ROUND(net_revenue_impact_denial.write_off_denial_amt, 3, 'ROUND_HALF_EVEN') AS write_off_denial_amt,
      ROUND(net_revenue_impact_denial.overturned_denial_amt, 3, 'ROUND_HALF_EVEN') AS overturned_denial_amt,
      net_revenue_impact_denial.dw_last_update_date_time,
      net_revenue_impact_denial.source_system_code
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.net_revenue_impact_denial
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_discrepancy_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.net_revenue_impact_discrepancy_inventory
   OPTIONS(description='Daily snapshot of Discrepancy Inventory to analyze the impact on Net Revenue')
  AS SELECT
      net_revenue_impact_discrepancy_inventory.reporting_date,
      ROUND(net_revenue_impact_discrepancy_inventory.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      net_revenue_impact_discrepancy_inventory.eor_log_date,
      net_revenue_impact_discrepancy_inventory.log_id,
      net_revenue_impact_discrepancy_inventory.log_sequence_num,
      net_revenue_impact_discrepancy_inventory.company_code,
      net_revenue_impact_discrepancy_inventory.coid,
      ROUND(net_revenue_impact_discrepancy_inventory.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      net_revenue_impact_discrepancy_inventory.iplan_insurance_order_num,
      net_revenue_impact_discrepancy_inventory.eff_from_date,
      ROUND(net_revenue_impact_discrepancy_inventory.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      net_revenue_impact_discrepancy_inventory.iplan_id,
      net_revenue_impact_discrepancy_inventory.remittance_date,
      net_revenue_impact_discrepancy_inventory.discrepancy_origination_date,
      net_revenue_impact_discrepancy_inventory.reason_assignment_date_1,
      net_revenue_impact_discrepancy_inventory.reason_assignment_date_2,
      net_revenue_impact_discrepancy_inventory.reason_assignment_date_3,
      net_revenue_impact_discrepancy_inventory.reason_assignment_date_4,
      ROUND(net_revenue_impact_discrepancy_inventory.over_under_payment_amt, 3, 'ROUND_HALF_EVEN') AS over_under_payment_amt,
      ROUND(net_revenue_impact_discrepancy_inventory.actual_payment_amt, 3, 'ROUND_HALF_EVEN') AS actual_payment_amt,
      ROUND(net_revenue_impact_discrepancy_inventory.var_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS var_total_charge_amt,
      ROUND(net_revenue_impact_discrepancy_inventory.var_gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_reimbursement_amt,
      ROUND(net_revenue_impact_discrepancy_inventory.var_primary_payor_pay_amt, 3, 'ROUND_HALF_EVEN') AS var_primary_payor_pay_amt,
      ROUND(net_revenue_impact_discrepancy_inventory.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      net_revenue_impact_discrepancy_inventory.inpatient_outpatient_code,
      net_revenue_impact_discrepancy_inventory.discrepancy_reason_code_1,
      net_revenue_impact_discrepancy_inventory.discrepancy_reason_code_2,
      net_revenue_impact_discrepancy_inventory.discrepancy_reason_code_3,
      net_revenue_impact_discrepancy_inventory.discrepancy_reason_code_4,
      net_revenue_impact_discrepancy_inventory.comment_text,
      net_revenue_impact_discrepancy_inventory.work_date,
      net_revenue_impact_discrepancy_inventory.last_racf_id,
      net_revenue_impact_discrepancy_inventory.last_racf_date,
      net_revenue_impact_discrepancy_inventory.data_source_code,
      ROUND(net_revenue_impact_discrepancy_inventory.cc_calc_id, 0, 'ROUND_HALF_EVEN') AS cc_calc_id,
      ROUND(net_revenue_impact_discrepancy_inventory.cc_account_activity_id, 0, 'ROUND_HALF_EVEN') AS cc_account_activity_id,
      ROUND(net_revenue_impact_discrepancy_inventory.cc_reason_id, 0, 'ROUND_HALF_EVEN') AS cc_reason_id,
      ROUND(net_revenue_impact_discrepancy_inventory.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id,
      net_revenue_impact_discrepancy_inventory.admission_date,
      net_revenue_impact_discrepancy_inventory.discharge_date,
      ROUND(net_revenue_impact_discrepancy_inventory.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      net_revenue_impact_discrepancy_inventory.patient_type_code,
      net_revenue_impact_discrepancy_inventory.ar_transaction_enter_date,
      net_revenue_impact_discrepancy_inventory.ar_transaction_effective_date,
      net_revenue_impact_discrepancy_inventory.take_back_ind,
      net_revenue_impact_discrepancy_inventory.denial_ind,
      net_revenue_impact_discrepancy_inventory.payment_type_ind,
      net_revenue_impact_discrepancy_inventory.cm_transaction_ind,
      net_revenue_impact_discrepancy_inventory.dw_last_update_date_time,
      net_revenue_impact_discrepancy_inventory.source_system_code
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.net_revenue_impact_discrepancy_inventory
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/net_revenue_impact_underpayment_pmt_act.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.net_revenue_impact_underpayment_pmt_act
   OPTIONS(description='Daily snapshot of Underpayment Recoveries to analyze the impact with Discrepancies raised and finally the impat on Net Revenue.\r\rThis table tracks the payment activity related to underpayment.')
  AS SELECT
      net_revenue_impact_underpayment_pmt_act.reporting_date,
      ROUND(net_revenue_impact_underpayment_pmt_act.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      ROUND(net_revenue_impact_underpayment_pmt_act.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      net_revenue_impact_underpayment_pmt_act.company_code,
      net_revenue_impact_underpayment_pmt_act.coid,
      net_revenue_impact_underpayment_pmt_act.admission_date,
      net_revenue_impact_underpayment_pmt_act.unit_num,
      ROUND(net_revenue_impact_underpayment_pmt_act.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      net_revenue_impact_underpayment_pmt_act.iplan_id,
      ROUND(net_revenue_impact_underpayment_pmt_act.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      net_revenue_impact_underpayment_pmt_act.patient_type_code,
      net_revenue_impact_underpayment_pmt_act.discrepancy_origination_date,
      net_revenue_impact_underpayment_pmt_act.discharge_date,
      ROUND(net_revenue_impact_underpayment_pmt_act.under_payment_activity_amt, 4, 'ROUND_HALF_EVEN') AS under_payment_activity_amt,
      net_revenue_impact_underpayment_pmt_act.payment_discrepancy_ind,
      net_revenue_impact_underpayment_pmt_act.take_back_ind,
      net_revenue_impact_underpayment_pmt_act.dw_last_update_date_time,
      net_revenue_impact_underpayment_pmt_act.source_system_code
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.net_revenue_impact_underpayment_pmt_act
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/parallon_client_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.parallon_client_detail AS SELECT
    parallon_client_detail.coid,
    parallon_client_detail.unit_num,
    parallon_client_detail.go_live_date,
    parallon_client_detail.ssc,
    parallon_client_detail.conversion_type,
    parallon_client_detail.company_code,
    parallon_client_detail.client_facility_id,
    parallon_client_detail.inbound_gl_ind
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.parallon_client_detail
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/pass_eom_lp.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.pass_eom_lp AS SELECT
    ROUND(pass_eom_lp.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    pass_eom_lp.company_code,
    pass_eom_lp.coid,
    pass_eom_lp.unit_num,
    pass_eom_lp.processing_zone_code,
    pass_eom_lp.stack_code,
    pass_eom_lp.rptg_period,
    ROUND(pass_eom_lp.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    pass_eom_lp.facility_name,
    pass_eom_lp.patient_full_name,
    ROUND(pass_eom_lp.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
    pass_eom_lp.financial_class_desc,
    pass_eom_lp.patient_type_code,
    pass_eom_lp.derived_patient_type_code,
    pass_eom_lp.account_status_code,
    pass_eom_lp.admission_date,
    pass_eom_lp.discharge_date,
    pass_eom_lp.final_bill_date,
    pass_eom_lp.ar_bill_thru_date,
    pass_eom_lp.length_of_stay_days_num,
    ROUND(pass_eom_lp.collector_org_code, 0, 'ROUND_HALF_EVEN') AS collector_org_code,
    pass_eom_lp.collector_org_type_code,
    pass_eom_lp.collector_org_short_name,
    pass_eom_lp.log_id,
    pass_eom_lp.log_name,
    pass_eom_lp.ip_contract_model_code,
    pass_eom_lp.iplan_id_ins1,
    pass_eom_lp.payor_name_ins1,
    pass_eom_lp.release_ind_ins1,
    ROUND(pass_eom_lp.payor_balance_amt_ins1, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_ins1,
    ROUND(pass_eom_lp.payor_liability_amt_ins1, 3, 'ROUND_HALF_EVEN') AS payor_liability_amt_ins1,
    ROUND(pass_eom_lp.payor_payment_amt_ins1, 3, 'ROUND_HALF_EVEN') AS payor_payment_amt_ins1,
    ROUND(pass_eom_lp.payor_adjustment_amt_ins1, 3, 'ROUND_HALF_EVEN') AS payor_adjustment_amt_ins1,
    ROUND(pass_eom_lp.payor_contract_allow_amt_ins1, 3, 'ROUND_HALF_EVEN') AS payor_contract_allow_amt_ins1,
    ROUND(pass_eom_lp.pa_syst_adj_current_amt_ins1, 3, 'ROUND_HALF_EVEN') AS pa_syst_adj_current_amt_ins1,
    ROUND(pass_eom_lp.pa_new_actv_current_amt_ins1, 3, 'ROUND_HALF_EVEN') AS pa_new_actv_current_amt_ins1,
    pass_eom_lp.iplan_id_ins2,
    pass_eom_lp.payor_name_ins2,
    pass_eom_lp.release_ind_ins2,
    ROUND(pass_eom_lp.payor_balance_amt_ins2, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_ins2,
    ROUND(pass_eom_lp.payor_liability_amt_ins2, 3, 'ROUND_HALF_EVEN') AS payor_liability_amt_ins2,
    ROUND(pass_eom_lp.payor_payment_amt_ins2, 3, 'ROUND_HALF_EVEN') AS payor_payment_amt_ins2,
    ROUND(pass_eom_lp.payor_adjustment_amt_ins2, 3, 'ROUND_HALF_EVEN') AS payor_adjustment_amt_ins2,
    ROUND(pass_eom_lp.payor_contract_allow_amt_ins2, 3, 'ROUND_HALF_EVEN') AS payor_contract_allow_amt_ins2,
    ROUND(pass_eom_lp.pa_syst_adj_current_amt_ins2, 3, 'ROUND_HALF_EVEN') AS pa_syst_adj_current_amt_ins2,
    ROUND(pass_eom_lp.pa_new_actv_current_amt_ins2, 3, 'ROUND_HALF_EVEN') AS pa_new_actv_current_amt_ins2,
    pass_eom_lp.iplan_id_ins3,
    pass_eom_lp.payor_name_ins3,
    pass_eom_lp.release_ind_ins3,
    ROUND(pass_eom_lp.payor_balance_amt_ins3, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_ins3,
    ROUND(pass_eom_lp.payor_liability_amt_ins3, 3, 'ROUND_HALF_EVEN') AS payor_liability_amt_ins3,
    ROUND(pass_eom_lp.payor_payment_amt_ins3, 3, 'ROUND_HALF_EVEN') AS payor_payment_amt_ins3,
    ROUND(pass_eom_lp.payor_adjustment_amt_ins3, 3, 'ROUND_HALF_EVEN') AS payor_adjustment_amt_ins3,
    ROUND(pass_eom_lp.payor_contract_allow_amt_ins3, 3, 'ROUND_HALF_EVEN') AS payor_contract_allow_amt_ins3,
    ROUND(pass_eom_lp.pa_syst_adj_current_amt_ins3, 3, 'ROUND_HALF_EVEN') AS pa_syst_adj_current_amt_ins3,
    ROUND(pass_eom_lp.pa_new_actv_current_amt_ins3, 3, 'ROUND_HALF_EVEN') AS pa_new_actv_current_amt_ins3,
    ROUND(pass_eom_lp.patient_balance_amt, 3, 'ROUND_HALF_EVEN') AS patient_balance_amt,
    ROUND(pass_eom_lp.patient_liability_amt, 3, 'ROUND_HALF_EVEN') AS patient_liability_amt,
    ROUND(pass_eom_lp.patient_payment_amt, 3, 'ROUND_HALF_EVEN') AS patient_payment_amt,
    ROUND(pass_eom_lp.patient_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS patient_adjustment_amt,
    ROUND(pass_eom_lp.patient_contract_allow_amt, 3, 'ROUND_HALF_EVEN') AS patient_contract_allow_amt,
    ROUND(pass_eom_lp.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
    ROUND(pass_eom_lp.total_rb_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_rb_charge_amt,
    ROUND(pass_eom_lp.total_anc_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_anc_charge_amt,
    ROUND(pass_eom_lp.total_write_off_bad_debt_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_bad_debt_amt,
    ROUND(pass_eom_lp.total_write_off_other_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_other_amt,
    ROUND(pass_eom_lp.total_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS total_adjustment_amt,
    ROUND(pass_eom_lp.total_contract_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_contract_allow_amt,
    ROUND(pass_eom_lp.total_combined_adj_alw_amt, 3, 'ROUND_HALF_EVEN') AS total_combined_adj_alw_amt,
    ROUND(pass_eom_lp.total_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_payment_amt,
    ROUND(pass_eom_lp.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
    ROUND(pass_eom_lp.prelim_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS prelim_account_balance_amt,
    ROUND(pass_eom_lp.eor_gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS eor_gross_reimbursement_amt,
    ROUND(pass_eom_lp.eor_contract_allow_amt, 3, 'ROUND_HALF_EVEN') AS eor_contract_allow_amt,
    ROUND(pass_eom_lp.eor_auto_post_amt, 3, 'ROUND_HALF_EVEN') AS eor_auto_post_amt,
    pass_eom_lp.discharge_to_eom_day_cnt,
    ROUND(pass_eom_lp.current_month_amt, 3, 'ROUND_HALF_EVEN') AS current_month_amt,
    pass_eom_lp.sub_unit_num,
    pass_eom_lp.sub_unit_name,
    pass_eom_lp.drg_code,
    pass_eom_lp.icd_version_ind,
    ROUND(pass_eom_lp.drg_payment_weight_amt, 5, 'ROUND_HALF_EVEN') AS drg_payment_weight_amt,
    pass_eom_lp.drg_code_hcfa,
    pass_eom_lp.drg_code_classic,
    pass_eom_lp.drg_code_pre_hac,
    pass_eom_lp.drg_code_pre_hac_tricare,
    pass_eom_lp.denial_code_ins1,
    ROUND(pass_eom_lp.financial_class_code_ins1, 0, 'ROUND_HALF_EVEN') AS financial_class_code_ins1,
    pass_eom_lp.denial_code_ins2,
    ROUND(pass_eom_lp.financial_class_code_ins2, 0, 'ROUND_HALF_EVEN') AS financial_class_code_ins2,
    pass_eom_lp.denial_code_ins3,
    ROUND(pass_eom_lp.financial_class_code_ins3, 0, 'ROUND_HALF_EVEN') AS financial_class_code_ins3,
    ROUND(pass_eom_lp.prorated_release_amt_ins1, 3, 'ROUND_HALF_EVEN') AS prorated_release_amt_ins1,
    ROUND(pass_eom_lp.prorated_release_amt_ins2, 3, 'ROUND_HALF_EVEN') AS prorated_release_amt_ins2,
    ROUND(pass_eom_lp.prorated_release_amt_ins3, 3, 'ROUND_HALF_EVEN') AS prorated_release_amt_ins3,
    pass_eom_lp.prorated_release_date_ins1,
    pass_eom_lp.prorated_release_date_ins2,
    pass_eom_lp.prorated_release_date_ins3,
    pass_eom_lp.logging_ind,
    pass_eom_lp.auto_post_ind,
    pass_eom_lp.unbill_pre_admit_ind,
    pass_eom_lp.insured_employer_name,
    pass_eom_lp.insured_role_code,
    pass_eom_lp.apr_severity_of_illness,
    pass_eom_lp.apr_risk_of_mortality
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.pass_eom_lp
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/pass_eow.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.pass_eow
   OPTIONS(description='A Patient Account Snapshot at weekly level. To analyze the AR Acccounts and its reimbursement.')
  AS SELECT
      pass_eow.company_code,
      pass_eow.coid,
      pass_eow.reporting_date,
      ROUND(pass_eow.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      pass_eow.log_id,
      pass_eow.iplan_id,
      pass_eow.patient_type_code,
      ROUND(pass_eow.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      pass_eow.unit_num,
      ROUND(pass_eow.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(pass_eow.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      pass_eow.final_bill_date,
      pass_eow.account_status_code,
      ROUND(pass_eow.total_billed_charge_amount, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amount,
      ROUND(pass_eow.eor_gross_reimbursement_amount, 3, 'ROUND_HALF_EVEN') AS eor_gross_reimbursement_amount,
      ROUND(pass_eow.total_account_balance_amount, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amount
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.pass_eow
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/pat_balance_liability.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.pat_balance_liability
   OPTIONS(description='Daily snapshot of patient balance and liability details from PA to drive SSC operations. This table will be truncated and loaded daily.')
  AS SELECT
      ROUND(pat_balance_liability.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      pat_balance_liability.insurance_order_num,
      ROUND(pat_balance_liability.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      pat_balance_liability.company_code,
      pat_balance_liability.coid,
      pat_balance_liability.unit_num,
      ROUND(pat_balance_liability.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      pat_balance_liability.iplan_identifier,
      ROUND(pat_balance_liability.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(pat_balance_liability.prorated_liability_amt, 3, 'ROUND_HALF_EVEN') AS prorated_liability_amt,
      ROUND(pat_balance_liability.cash_amt, 3, 'ROUND_HALF_EVEN') AS cash_amt,
      ROUND(pat_balance_liability.bill_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS bill_adjustment_amt,
      ROUND(pat_balance_liability.allowance_amt, 3, 'ROUND_HALF_EVEN') AS allowance_amt,
      ROUND(pat_balance_liability.nonbill_adj_amt, 3, 'ROUND_HALF_EVEN') AS nonbill_adj_amt,
      ROUND(pat_balance_liability.bal_due_amt, 3, 'ROUND_HALF_EVEN') AS bal_due_amt,
      ROUND(pat_balance_liability.sys_adj_amt, 3, 'ROUND_HALF_EVEN') AS sys_adj_amt,
      pat_balance_liability.source_system_code,
      pat_balance_liability.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.pat_balance_liability
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_account_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.patient_account_detail
   OPTIONS(description='Daily snapshot of patient account details from PA to drive SSC operations. This table will be truncated and loaded daily.')
  AS SELECT
      ROUND(patient_account_detail.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      patient_account_detail.company_code,
      patient_account_detail.coid,
      patient_account_detail.sub_unit_num,
      ROUND(patient_account_detail.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      patient_account_detail.medical_record_num,
      patient_account_detail.patient_type_code,
      ROUND(patient_account_detail.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      patient_account_detail.account_status_code,
      patient_account_detail.admission_type_code,
      patient_account_detail.admission_source_code,
      patient_account_detail.patient_full_name,
      patient_account_detail.social_security_num,
      patient_account_detail.birth_date,
      patient_account_detail.gender_code,
      patient_account_detail.marital_status_code,
      ROUND(patient_account_detail.patient_person_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_person_dw_id,
      ROUND(patient_account_detail.patient_address_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_address_dw_id,
      patient_account_detail.admission_date,
      patient_account_detail.discharge_date,
      patient_account_detail.final_bill_date,
      patient_account_detail.los_day_cnt,
      patient_account_detail.drg_code,
      patient_account_detail.principal_diag_code_icd9,
      patient_account_detail.principal_diag_desc_icd9,
      patient_account_detail.principal_diag_code_icd10,
      patient_account_detail.principal_diag_desc_icd10,
      patient_account_detail.service_code,
      patient_account_detail.accomodation_code,
      patient_account_detail.room_num,
      patient_account_detail.bill_gen_date,
      patient_account_detail.responsible_party_name,
      patient_account_detail.pat_relationship_code,
      patient_account_detail.agency_code,
      patient_account_detail.initial_turnover_date,
      patient_account_detail.stmt_issue_date,
      ROUND(patient_account_detail.patient_allowance_amt, 3, 'ROUND_HALF_EVEN') AS patient_allowance_amt,
      ROUND(patient_account_detail.patient_adj_amt, 3, 'ROUND_HALF_EVEN') AS patient_adj_amt,
      ROUND(patient_account_detail.non_billable_adj_amt, 3, 'ROUND_HALF_EVEN') AS non_billable_adj_amt,
      ROUND(patient_account_detail.non_billable_charge_amt, 3, 'ROUND_HALF_EVEN') AS non_billable_charge_amt,
      ROUND(patient_account_detail.patient_balance_amt, 3, 'ROUND_HALF_EVEN') AS patient_balance_amt,
      ROUND(patient_account_detail.last_patient_pmt_amt, 3, 'ROUND_HALF_EVEN') AS last_patient_pmt_amt,
      patient_account_detail.last_patient_pmt_date,
      patient_account_detail.pmt_arng_stmt_freq,
      patient_account_detail.pmt_arng_start_date,
      ROUND(patient_account_detail.previous_due_amt, 3, 'ROUND_HALF_EVEN') AS previous_due_amt,
      ROUND(patient_account_detail.current_due_amt, 3, 'ROUND_HALF_EVEN') AS current_due_amt,
      ROUND(patient_account_detail.patient_overdue_amt, 3, 'ROUND_HALF_EVEN') AS patient_overdue_amt,
      ROUND(patient_account_detail.patient_min_due_amt, 3, 'ROUND_HALF_EVEN') AS patient_min_due_amt,
      ROUND(patient_account_detail.total_patient_pmt_amt, 3, 'ROUND_HALF_EVEN') AS total_patient_pmt_amt,
      ROUND(patient_account_detail.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(patient_account_detail.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(patient_account_detail.total_pmt_amt, 3, 'ROUND_HALF_EVEN') AS total_pmt_amt,
      ROUND(patient_account_detail.total_allowance_amt, 3, 'ROUND_HALF_EVEN') AS total_allowance_amt,
      ROUND(patient_account_detail.total_write_off_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_amt,
      ROUND(patient_account_detail.total_adj_amt, 3, 'ROUND_HALF_EVEN') AS total_adj_amt,
      patient_account_detail.stmt_cnt,
      patient_account_detail.last_pmt_rcvd_date,
      patient_account_detail.recall_date,
      patient_account_detail.initial_bad_debt_prelist_date,
      patient_account_detail.bad_debt_reason_code,
      patient_account_detail.patient_turnover_date,
      patient_account_detail.agency_type_code,
      patient_account_detail.current_collection_series_num,
      patient_account_detail.previous_collection_series_num,
      patient_account_detail.pa_restore_date,
      patient_account_detail.source_system_code,
      patient_account_detail.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.patient_account_detail
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_insurance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.patient_insurance
   OPTIONS(description='Daily snapshot of  patient Insurance related information  from PA')
  AS SELECT
      ROUND(patient_insurance.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      patient_insurance.insurance_order_num,
      ROUND(patient_insurance.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      patient_insurance.group_name,
      patient_insurance.group_num,
      patient_insurance.coid,
      patient_insurance.company_code,
      patient_insurance.iplan_id,
      ROUND(patient_insurance.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      patient_insurance.hic_claim_num,
      patient_insurance.claim_submit_date,
      patient_insurance.denial_code,
      patient_insurance.denial_status_code,
      patient_insurance.payor_cont_auto_post_ind,
      patient_insurance.mail_to_name,
      ROUND(patient_insurance.address_dw_id, 0, 'ROUND_HALF_EVEN') AS address_dw_id,
      patient_insurance.log_id,
      ROUND(patient_insurance.allowance_amt, 3, 'ROUND_HALF_EVEN') AS allowance_amt,
      ROUND(patient_insurance.adj_amt, 3, 'ROUND_HALF_EVEN') AS adj_amt,
      ROUND(patient_insurance.payor_balance_amt, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt,
      ROUND(patient_insurance.last_pmt_rcvd_amt, 3, 'ROUND_HALF_EVEN') AS last_pmt_rcvd_amt,
      patient_insurance.last_pmt_rcvd_date,
      ROUND(patient_insurance.total_pmt_rcvd_amt, 3, 'ROUND_HALF_EVEN') AS total_pmt_rcvd_amt,
      ROUND(patient_insurance.gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS gross_reimbursement_amt,
      ROUND(patient_insurance.gross_reimbursement_var_amt, 3, 'ROUND_HALF_EVEN') AS gross_reimbursement_var_amt,
      ROUND(patient_insurance.payor_liability_amt, 3, 'ROUND_HALF_EVEN') AS payor_liability_amt,
      patient_insurance.source_system_code,
      patient_insurance.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.patient_insurance
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_compliance_appeal_asst_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.payment_compliance_appeal_asst_inventory
   OPTIONS(description='Denials and Appeals Inventory from Appeal Assistant tool for corporate payment compliance reporting and reconciliation')
  AS SELECT
      ROUND(payment_compliance_appeal_asst_inventory.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      payment_compliance_appeal_asst_inventory.last_appeal_attempt_key_num,
      payment_compliance_appeal_asst_inventory.appeal_form_downloaded_flag,
      payment_compliance_appeal_asst_inventory.reporting_date,
      payment_compliance_appeal_asst_inventory.inventory_date,
      payment_compliance_appeal_asst_inventory.appeal_status_desc,
      payment_compliance_appeal_asst_inventory.appeal_level_num,
      payment_compliance_appeal_asst_inventory.iplan_id,
      payment_compliance_appeal_asst_inventory.iplan_insurance_order_num,
      payment_compliance_appeal_asst_inventory.major_payor_group_id,
      payment_compliance_appeal_asst_inventory.coid,
      payment_compliance_appeal_asst_inventory.company_code,
      ROUND(payment_compliance_appeal_asst_inventory.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      payment_compliance_appeal_asst_inventory.unit_num,
      payment_compliance_appeal_asst_inventory.patient_type_code,
      payment_compliance_appeal_asst_inventory.meditech_acct_num,
      payment_compliance_appeal_asst_inventory.appeal_disp_desc,
      payment_compliance_appeal_asst_inventory.denial_code,
      payment_compliance_appeal_asst_inventory.denial_code_group_name,
      payment_compliance_appeal_asst_inventory.denial_src_system_code,
      payment_compliance_appeal_asst_inventory.service_code,
      payment_compliance_appeal_asst_inventory.denial_key_num,
      payment_compliance_appeal_asst_inventory.denial_src_disp_desc,
      payment_compliance_appeal_asst_inventory.appeal_rule_key_num,
      payment_compliance_appeal_asst_inventory.appeal_rule_id,
      payment_compliance_appeal_asst_inventory.patient_full_name,
      payment_compliance_appeal_asst_inventory.patient_birth_date,
      ROUND(payment_compliance_appeal_asst_inventory.payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_code,
      payment_compliance_appeal_asst_inventory.claim_id,
      payment_compliance_appeal_asst_inventory.hic_claim_num,
      payment_compliance_appeal_asst_inventory.application_name,
      payment_compliance_appeal_asst_inventory.appeal_folder_status,
      payment_compliance_appeal_asst_inventory.appeal_key_num,
      payment_compliance_appeal_asst_inventory.appeal_workflow_comment,
      payment_compliance_appeal_asst_inventory.appeal_guid,
      payment_compliance_appeal_asst_inventory.appeal_evnt_error_desc,
      payment_compliance_appeal_asst_inventory.appeal_evnt_error_reason_desc,
      payment_compliance_appeal_asst_inventory.src_appeal_num,
      payment_compliance_appeal_asst_inventory.onbase_document_id,
      payment_compliance_appeal_asst_inventory.acct_selected_ind,
      payment_compliance_appeal_asst_inventory.hpf_acct_ind,
      payment_compliance_appeal_asst_inventory.hpf_page_total_num,
      payment_compliance_appeal_asst_inventory.acct_sent_to_print_ind,
      payment_compliance_appeal_asst_inventory.sent_to_print_page_total_num,
      payment_compliance_appeal_asst_inventory.acct_confirmed_print_ind,
      payment_compliance_appeal_asst_inventory.confirmed_print_page_total_num,
      payment_compliance_appeal_asst_inventory.acct_onbase_upload_ind,
      payment_compliance_appeal_asst_inventory.onbase_upload_page_total_num,
      payment_compliance_appeal_asst_inventory.hpf_required_ind,
      payment_compliance_appeal_asst_inventory.hpf_downloaded_flag,
      payment_compliance_appeal_asst_inventory.hpf_page_cnt,
      ROUND(payment_compliance_appeal_asst_inventory.appeal_amt, 3, 'ROUND_HALF_EVEN') AS appeal_amt,
      ROUND(payment_compliance_appeal_asst_inventory.denied_charge_amt, 3, 'ROUND_HALF_EVEN') AS denied_charge_amt,
      ROUND(payment_compliance_appeal_asst_inventory.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      payment_compliance_appeal_asst_inventory.account_processed_cnt,
      payment_compliance_appeal_asst_inventory.appeal_level_origination_date_time,
      payment_compliance_appeal_asst_inventory.appeal_origination_date,
      payment_compliance_appeal_asst_inventory.admission_date,
      payment_compliance_appeal_asst_inventory.discharge_date,
      payment_compliance_appeal_asst_inventory.onbase_upload_date_time,
      payment_compliance_appeal_asst_inventory.sent_to_print_date_time,
      payment_compliance_appeal_asst_inventory.confirmed_print_date_time,
      payment_compliance_appeal_asst_inventory.appeal_evnt_modified_date_time,
      payment_compliance_appeal_asst_inventory.appeal_evnt_type_key_num,
      payment_compliance_appeal_asst_inventory.appeal_evnt_log_key_num,
      payment_compliance_appeal_asst_inventory.appeal_status_modified_date_time,
      payment_compliance_appeal_asst_inventory.appeal_evnt_error_date_time,
      payment_compliance_appeal_asst_inventory.appeal_evnt_error_category_name,
      payment_compliance_appeal_asst_inventory.appeal_rule_template_name,
      payment_compliance_appeal_asst_inventory.template_download_date_time,
      payment_compliance_appeal_asst_inventory.hpf_selection_date_time,
      payment_compliance_appeal_asst_inventory.hpf_download_date_time,
      payment_compliance_appeal_asst_inventory.appeal_form_viewed_date_time,
      payment_compliance_appeal_asst_inventory.appeal_form_download_date_time,
      payment_compliance_appeal_asst_inventory.appeal_created_by_user_id,
      payment_compliance_appeal_asst_inventory.appeal_created_date_time,
      payment_compliance_appeal_asst_inventory.appeal_uploaded_date_time,
      payment_compliance_appeal_asst_inventory.appeal_uploaded_by_user_id,
      payment_compliance_appeal_asst_inventory.template_download_by_user_id,
      payment_compliance_appeal_asst_inventory.source_system_code,
      payment_compliance_appeal_asst_inventory.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.payment_compliance_appeal_asst_inventory
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_compliance_credit_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.payment_compliance_credit_inventory
   OPTIONS(description='This is a daily table that contains Credit Inventory of all the accounts showing Refund amounts and Credit Balance Amounts and other details.')
  AS SELECT
      ROUND(payment_compliance_credit_inventory.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      payment_compliance_credit_inventory.reporting_date,
      payment_compliance_credit_inventory.credit_balance_refund_ind,
      payment_compliance_credit_inventory.credit_balance_refund_id,
      payment_compliance_credit_inventory.company_code,
      payment_compliance_credit_inventory.coid,
      ROUND(payment_compliance_credit_inventory.payor_dw_id_ins1, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins1,
      payment_compliance_credit_inventory.unit_num,
      ROUND(payment_compliance_credit_inventory.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      payment_compliance_credit_inventory.admission_type_code,
      payment_compliance_credit_inventory.account_status_code,
      payment_compliance_credit_inventory.refund_type_sid,
      payment_compliance_credit_inventory.credit_status_sid,
      ROUND(payment_compliance_credit_inventory.payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_code,
      payment_compliance_credit_inventory.iplan_id_ins1,
      payment_compliance_credit_inventory.iplan_id_ins2,
      payment_compliance_credit_inventory.iplan_id_ins3,
      ROUND(payment_compliance_credit_inventory.patient_person_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_person_dw_id,
      ROUND(payment_compliance_credit_inventory.patient_address_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_address_dw_id,
      payment_compliance_credit_inventory.member_id,
      payment_compliance_credit_inventory.logged_ind,
      payment_compliance_credit_inventory.refund_iplan_id,
      payment_compliance_credit_inventory.refund_special_code,
      payment_compliance_credit_inventory.refund_procedure_code,
      payment_compliance_credit_inventory.refund_gl_acct_num,
      ROUND(payment_compliance_credit_inventory.refund_payor_dw_id, 0, 'ROUND_HALF_EVEN') AS refund_payor_dw_id,
      ROUND(payment_compliance_credit_inventory.refund_payor_address_dw_id, 0, 'ROUND_HALF_EVEN') AS refund_payor_address_dw_id,
      payment_compliance_credit_inventory.refund_creation_date_time,
      payment_compliance_credit_inventory.refund_creation_user_id,
      ROUND(payment_compliance_credit_inventory.refund_amt, 3, 'ROUND_HALF_EVEN') AS refund_amt,
      ROUND(payment_compliance_credit_inventory.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(payment_compliance_credit_inventory.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(payment_compliance_credit_inventory.total_cash_pay_amt, 3, 'ROUND_HALF_EVEN') AS total_cash_pay_amt,
      ROUND(payment_compliance_credit_inventory.total_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_allow_amt,
      ROUND(payment_compliance_credit_inventory.total_policy_adj_amt, 3, 'ROUND_HALF_EVEN') AS total_policy_adj_amt,
      ROUND(payment_compliance_credit_inventory.total_write_off_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_amt,
      payment_compliance_credit_inventory.credit_balance_date,
      ROUND(payment_compliance_credit_inventory.current_grv_amt, 3, 'ROUND_HALF_EVEN') AS current_grv_amt,
      payment_compliance_credit_inventory.entered_date,
      payment_compliance_credit_inventory.discharge_date,
      payment_compliance_credit_inventory.admission_date,
      payment_compliance_credit_inventory.final_bill_date,
      payment_compliance_credit_inventory.bill_through_date,
      payment_compliance_credit_inventory.credit_bal_estb_date_time,
      payment_compliance_credit_inventory.last_update_date_time,
      payment_compliance_credit_inventory.last_update_user_id,
      payment_compliance_credit_inventory.approved_by_user_id,
      payment_compliance_credit_inventory.approved_date_time,
      payment_compliance_credit_inventory.auto_refund_create_date_time,
      payment_compliance_credit_inventory.resolved_date,
      payment_compliance_credit_inventory.eligible_transfer_ind,
      payment_compliance_credit_inventory.late_charge_ind,
      payment_compliance_credit_inventory.restore_date_time,
      payment_compliance_credit_inventory.restore_ind,
      payment_compliance_credit_inventory.source_system_code,
      payment_compliance_credit_inventory.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.payment_compliance_credit_inventory
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_compliance_legacy_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.payment_compliance_legacy_denial
   OPTIONS(description='Legacy SSC denials (open & closed) for corporate payment compliance reporting and reconciliation')
  AS SELECT
      ROUND(payment_compliance_legacy_denial.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      payment_compliance_legacy_denial.iplan_id,
      payment_compliance_legacy_denial.iplan_insurance_order_num,
      payment_compliance_legacy_denial.appeal_level_num,
      payment_compliance_legacy_denial.reporting_date,
      payment_compliance_legacy_denial.coid,
      payment_compliance_legacy_denial.company_code,
      ROUND(payment_compliance_legacy_denial.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      payment_compliance_legacy_denial.unit_num,
      payment_compliance_legacy_denial.appeal_disp_code,
      payment_compliance_legacy_denial.denial_code,
      payment_compliance_legacy_denial.web_disp_type_code,
      ROUND(payment_compliance_legacy_denial.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(payment_compliance_legacy_denial.acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS acct_bal_amt,
      ROUND(payment_compliance_legacy_denial.payor_bal_amt, 3, 'ROUND_HALF_EVEN') AS payor_bal_amt,
      payment_compliance_legacy_denial.appeal_root_cause_sid,
      ROUND(payment_compliance_legacy_denial.appeal_amt, 3, 'ROUND_HALF_EVEN') AS appeal_amt,
      ROUND(payment_compliance_legacy_denial.appeal_crnt_bal_amt, 3, 'ROUND_HALF_EVEN') AS appeal_crnt_bal_amt,
      ROUND(payment_compliance_legacy_denial.overturned_acct_amt, 3, 'ROUND_HALF_EVEN') AS overturned_acct_amt,
      ROUND(payment_compliance_legacy_denial.write_off_denial_account_amt, 3, 'ROUND_HALF_EVEN') AS write_off_denial_account_amt,
      ROUND(payment_compliance_legacy_denial.xfer_next_party_amt, 3, 'ROUND_HALF_EVEN') AS xfer_next_party_amt,
      ROUND(payment_compliance_legacy_denial.denied_charge_amt, 3, 'ROUND_HALF_EVEN') AS denied_charge_amt,
      payment_compliance_legacy_denial.denial_date,
      payment_compliance_legacy_denial.appeal_level_origination_date_time,
      payment_compliance_legacy_denial.appeal_deadline_date,
      payment_compliance_legacy_denial.appeal_closing_date,
      payment_compliance_legacy_denial.work_again_date,
      payment_compliance_legacy_denial.appeal_origination_date,
      payment_compliance_legacy_denial.appeal_assigned_user_id,
      payment_compliance_legacy_denial.pa_vendor_code,
      payment_compliance_legacy_denial.source_system_code,
      payment_compliance_legacy_denial.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.payment_compliance_legacy_denial
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_compliance_payor_address.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.payment_compliance_payor_address
   OPTIONS(description='Payor Address master table for appeals, underpayment, overpayments, and other non-claim related addresses from Payment Compliance')
  AS SELECT
      payment_compliance_payor_address.address_sid,
      payment_compliance_payor_address.major_payor_group_id,
      payment_compliance_payor_address.salutation_name,
      payment_compliance_payor_address.payor_name,
      payment_compliance_payor_address.address_1_text,
      payment_compliance_payor_address.address_2_text,
      payment_compliance_payor_address.city_name,
      payment_compliance_payor_address.state_code,
      payment_compliance_payor_address.zip_code,
      ROUND(payment_compliance_payor_address.fax_num, 0, 'ROUND_HALF_EVEN') AS fax_num,
      payment_compliance_payor_address.email_text,
      payment_compliance_payor_address.eff_start_date,
      payment_compliance_payor_address.eff_end_date,
      payment_compliance_payor_address.source_system_code,
      payment_compliance_payor_address.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.payment_compliance_payor_address
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_resolution_recovery_adj_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.payment_resolution_recovery_adj_dtl
   OPTIONS(description='Payment resolution adjustment details for all recoveries captured by Payment Resolution Application is maintained in the table.')
  AS SELECT
      payment_resolution_recovery_adj_dtl.payment_resolution_recovery_adj_id,
      payment_resolution_recovery_adj_dtl.reporting_date,
      payment_resolution_recovery_adj_dtl.rpt_freq_type_code,
      payment_resolution_recovery_adj_dtl.payment_resolution_recovery_id,
      payment_resolution_recovery_adj_dtl.company_code,
      payment_resolution_recovery_adj_dtl.coid,
      payment_resolution_recovery_adj_dtl.entry_type_code,
      payment_resolution_recovery_adj_dtl.reporting_period,
      payment_resolution_recovery_adj_dtl.valid_recovery_ind,
      ROUND(payment_resolution_recovery_adj_dtl.collection_amt, 3, 'ROUND_HALF_EVEN') AS collection_amt,
      ROUND(payment_resolution_recovery_adj_dtl.net_collection_amt, 3, 'ROUND_HALF_EVEN') AS net_collection_amt,
      payment_resolution_recovery_adj_dtl.executive_approval_status_ind,
      payment_resolution_recovery_adj_dtl.created_by_user_id,
      payment_resolution_recovery_adj_dtl.created_date_time,
      payment_resolution_recovery_adj_dtl.updated_by_user_id,
      payment_resolution_recovery_adj_dtl.updated_by_date_time,
      payment_resolution_recovery_adj_dtl.recovery_comment_text,
      payment_resolution_recovery_adj_dtl.source_system_code,
      payment_resolution_recovery_adj_dtl.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.payment_resolution_recovery_adj_dtl
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_resolution_recovery_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.payment_resolution_recovery_dtl
   OPTIONS(description='Payment resolution amounts captured by Payment Resolution Application at the transaction level is maintained in the table.')
  AS SELECT
      payment_resolution_recovery_dtl.payment_resolution_recovery_id,
      payment_resolution_recovery_dtl.reporting_date,
      payment_resolution_recovery_dtl.rpt_freq_type_code,
      ROUND(payment_resolution_recovery_dtl.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(payment_resolution_recovery_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      payment_resolution_recovery_dtl.company_code,
      payment_resolution_recovery_dtl.coid,
      payment_resolution_recovery_dtl.unit_num,
      payment_resolution_recovery_dtl.created_by_user_id,
      payment_resolution_recovery_dtl.created_date_time,
      payment_resolution_recovery_dtl.file_id,
      payment_resolution_recovery_dtl.invalid_ind,
      payment_resolution_recovery_dtl.month_id,
      payment_resolution_recovery_dtl.iplan_id,
      ROUND(payment_resolution_recovery_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      payment_resolution_recovery_dtl.financial_class_sid,
      payment_resolution_recovery_dtl.patient_type_sid,
      payment_resolution_recovery_dtl.payment_date,
      payment_resolution_recovery_dtl.payor_sid,
      ROUND(payment_resolution_recovery_dtl.reason_code_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_sid,
      payment_resolution_recovery_dtl.updated_by_user_id,
      payment_resolution_recovery_dtl.updated_by_date_time,
      payment_resolution_recovery_dtl.source_system_code,
      payment_resolution_recovery_dtl.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.payment_resolution_recovery_dtl
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payor_audit_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.payor_audit_detail
   OPTIONS(description='Table that captures the Patient Account details and Audit information that is requested for all accounts by Payors and tracked in application during our billing cycles.')
  AS SELECT
      payor_audit_detail.audit_record_sid,
      ROUND(payor_audit_detail.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      payor_audit_detail.reporting_date,
      payor_audit_detail.daily_month_code,
      payor_audit_detail.account_create_timestamp,
      payor_audit_detail.appeal_create_timestamp,
      payor_audit_detail.doc_ctrl_num,
      payor_audit_detail.last_appeal_update_timestamp,
      payor_audit_detail.last_appeal_update_user_id,
      payor_audit_detail.work_queue_desc,
      payor_audit_detail.company_code,
      payor_audit_detail.coid,
      payor_audit_detail.unit_num,
      ROUND(payor_audit_detail.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      payor_audit_detail.month_id,
      payor_audit_detail.audit_create_date,
      payor_audit_detail.audit_create_user_id,
      payor_audit_detail.financial_class_code,
      payor_audit_detail.major_payor_code,
      payor_audit_detail.insurance_order_num,
      payor_audit_detail.iplan_id,
      payor_audit_detail.letter_date,
      payor_audit_detail.category_desc,
      payor_audit_detail.worklist_desc,
      payor_audit_detail.workflow_type_desc,
      payor_audit_detail.audit_start_date,
      payor_audit_detail.audit_initiation_date,
      payor_audit_detail.medical_rec_requestor_entity_desc,
      payor_audit_detail.audit_type_desc,
      payor_audit_detail.other_audit_desc,
      payor_audit_detail.service_start_date,
      payor_audit_detail.service_end_date,
      payor_audit_detail.medical_record_due_date,
      payor_audit_detail.audit_location_desc,
      payor_audit_detail.audit_eligibility_desc,
      payor_audit_detail.audit_fee_required_ind,
      payor_audit_detail.previous_audit_status_desc,
      payor_audit_detail.audit_work_queue_desc,
      payor_audit_detail.audit_status_desc,
      payor_audit_detail.audit_status_date,
      payor_audit_detail.last_audit_status_update_date,
      payor_audit_detail.last_audit_status_update_user_id,
      payor_audit_detail.post_audit_type_desc,
      payor_audit_detail.work_again_date,
      payor_audit_detail.corporate_status_desc,
      payor_audit_detail.corporate_status_date,
      payor_audit_detail.letter_result_desc,
      payor_audit_detail.final_audit_outcome_desc,
      payor_audit_detail.final_audit_outcome_date,
      payor_audit_detail.corp_doc_control_code,
      payor_audit_detail.ssc_doc_control_code,
      payor_audit_detail.missing_doc_cnt,
      payor_audit_detail.audit_schedule_date,
      payor_audit_detail.medical_rec_release_method_desc,
      payor_audit_detail.medical_rec_tracking_num,
      payor_audit_detail.medical_rec_release_request_date,
      payor_audit_detail.medical_rec_sent_date,
      payor_audit_detail.medical_rec_missing_notification_date,
      payor_audit_detail.payor_reference_id,
      ROUND(payor_audit_detail.collected_audit_fee_amt, 3, 'ROUND_HALF_EVEN') AS collected_audit_fee_amt,
      payor_audit_detail.additional_project_name,
      payor_audit_detail.recurring_account_sw,
      ROUND(payor_audit_detail.risk_amt, 3, 'ROUND_HALF_EVEN') AS risk_amt,
      ROUND(payor_audit_detail.charge_reviewed_amt, 3, 'ROUND_HALF_EVEN') AS charge_reviewed_amt,
      ROUND(payor_audit_detail.current_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS current_total_charge_amt,
      ROUND(payor_audit_detail.current_total_charge_discrepancy_amt, 3, 'ROUND_HALF_EVEN') AS current_total_charge_discrepancy_amt,
      ROUND(payor_audit_detail.expected_payor_payment_amt, 3, 'ROUND_HALF_EVEN') AS expected_payor_payment_amt,
      ROUND(payor_audit_detail.current_payor_payment_discrepancy_amt, 3, 'ROUND_HALF_EVEN') AS current_payor_payment_discrepancy_amt,
      ROUND(payor_audit_detail.initial_payor_payment_amt, 3, 'ROUND_HALF_EVEN') AS initial_payor_payment_amt,
      ROUND(payor_audit_detail.current_payor_payment_amt, 3, 'ROUND_HALF_EVEN') AS current_payor_payment_amt,
      payor_audit_detail.pre_audit_drg_code,
      payor_audit_detail.pre_audit_drg_desc,
      payor_audit_detail.payor_drg_code,
      payor_audit_detail.payer_drg_desc,
      payor_audit_detail.claim_guid,
      payor_audit_detail.post_audit_drg,
      payor_audit_detail.post_audit_drg_desc,
      payor_audit_detail.appeal_create_user_id,
      payor_audit_detail.appeal_level_1_due_date,
      payor_audit_detail.appeal_level_1_letter_sent_date,
      payor_audit_detail.appeal_level_1_followup_date,
      payor_audit_detail.appeal_level_1_response_date,
      payor_audit_detail.appeal_level_1_decision_desc,
      payor_audit_detail.appeal_level_2_due_date,
      payor_audit_detail.appeal_level_2_letter_sent_date,
      payor_audit_detail.appeal_level_2_followup_date,
      payor_audit_detail.appeal_level_2_response_date,
      payor_audit_detail.appeal_level_2_decision_desc,
      payor_audit_detail.appeal_level_3_due_date,
      payor_audit_detail.appeal_level_3_letter_sent_date,
      payor_audit_detail.appeal_level_3_followup_date,
      payor_audit_detail.appeal_level_3_response_date,
      payor_audit_detail.appeal_level_3_decision_desc,
      payor_audit_detail.appeal_level_4_due_date,
      payor_audit_detail.appeal_level_4_letter_sent_date,
      payor_audit_detail.appeal_level_4_followup_date,
      payor_audit_detail.appeal_level_4_response_date,
      payor_audit_detail.appeal_level_4_decision_desc,
      payor_audit_detail.appeal_level_5_due_date,
      payor_audit_detail.appeal_level_5_letter_sent_date,
      payor_audit_detail.appeal_level_5_followup_date,
      payor_audit_detail.appeal_level_5_response_date,
      payor_audit_detail.appeal_level_5_decision_desc,
      payor_audit_detail.source_system_code,
      payor_audit_detail.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.payor_audit_detail
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payor_employer.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.payor_employer
   OPTIONS(description='This table captures employer and employee relationship for the insurance reported.')
  AS SELECT
      payor_employer.company_code,
      payor_employer.coid,
      ROUND(payor_employer.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      payor_employer.iplan_id,
      payor_employer.employee_code,
      payor_employer.iplan_insurance_order_num,
      payor_employer.unit_num,
      payor_employer.employee_id,
      ROUND(payor_employer.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ROUND(payor_employer.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      payor_employer.employee_work_area_code_num,
      payor_employer.employee_work_telephone,
      payor_employer.employment_status_code,
      payor_employer.employer_area_code_num,
      payor_employer.employer_name,
      payor_employer.employer_short_name,
      payor_employer.employer_street1_address,
      payor_employer.employer_street2_address,
      payor_employer.employer_city_name,
      payor_employer.employer_state_code,
      payor_employer.employer_zip_code,
      payor_employer.employer_telephone_num,
      payor_employer.length_of_employment_num,
      payor_employer.occupation_desc,
      payor_employer.pa_last_update_date,
      payor_employer.source_system_code,
      payor_employer.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.payor_employer
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payor_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.payor_eom
   OPTIONS(description='Payor end of month snap shot table which contains monthly changes to payor members and keeps history')
  AS SELECT
      payor_eom.pe_date,
      ROUND(payor_eom.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      payor_eom.coid,
      payor_eom.company_code,
      payor_eom.iplan_id,
      ROUND(payor_eom.iplan_financial_class_code, 0, 'ROUND_HALF_EVEN') AS iplan_financial_class_code,
      payor_eom.sub_payor_group_id,
      payor_eom.major_payor_group_id,
      payor_eom.payor_sid,
      payor_eom.payor_id,
      payor_eom.unit_num,
      payor_eom.plan_desc,
      payor_eom.sub_payor_group_desc,
      payor_eom.major_payor_group_desc,
      payor_eom.payor_gen_02_code,
      payor_eom.bankrupt_payor_ind,
      payor_eom.auto_payor_ind,
      payor_eom.mcaid_pending_payor_ind,
      payor_eom.major_payor_group_unique_num,
      ROUND(payor_eom.calc_rev_cat_financial_class_code, 0, 'ROUND_HALF_EVEN') AS calc_rev_cat_financial_class_code,
      payor_eom.meditech_mnemonic,
      payor_eom.payer_type_code,
      payor_eom.eff_from_date,
      payor_eom.eff_to_date,
      payor_eom.source_system_code,
      payor_eom.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.payor_eom
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/post_write_off_rebill_hist_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.post_write_off_rebill_hist_dtl
   OPTIONS(description='This table contains all Post Write Off Rebill History Detail of the accounts')
  AS SELECT
      ROUND(post_write_off_rebill_hist_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      post_write_off_rebill_hist_dtl.denial_orig_age_month_id,
      post_write_off_rebill_hist_dtl.write_off_pe_date,
      post_write_off_rebill_hist_dtl.company_code,
      post_write_off_rebill_hist_dtl.coid,
      post_write_off_rebill_hist_dtl.unit_num,
      ROUND(post_write_off_rebill_hist_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      post_write_off_rebill_hist_dtl.midas_acct_num,
      ROUND(post_write_off_rebill_hist_dtl.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(post_write_off_rebill_hist_dtl.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      post_write_off_rebill_hist_dtl.iplan_id,
      post_write_off_rebill_hist_dtl.discharge_date,
      post_write_off_rebill_hist_dtl.ms_drg_med_surg_flag,
      post_write_off_rebill_hist_dtl.denial_escl_review_desc,
      post_write_off_rebill_hist_dtl.denial_escl_rebill_flag,
      post_write_off_rebill_hist_dtl.denial_escl_rebill_project_date,
      post_write_off_rebill_hist_dtl.procedure_code,
      ROUND(post_write_off_rebill_hist_dtl.initial_write_off_amt, 3, 'ROUND_HALF_EVEN') AS initial_write_off_amt,
      post_write_off_rebill_hist_dtl.source_system_code,
      post_write_off_rebill_hist_dtl.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.post_write_off_rebill_hist_dtl
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/prebill_denial_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.prebill_denial_detail
   OPTIONS(description='PreBill Denial Inventory for Parallon Customers. This is will have a monthly data and daily data for 7 days rolling.')
  AS SELECT
      prebill_denial_detail.company_code,
      prebill_denial_detail.coid,
      prebill_denial_detail.rptg_date,
      prebill_denial_detail.account_id,
      ROUND(prebill_denial_detail.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      prebill_denial_detail.unit_num,
      prebill_denial_detail.facility_tax_id,
      prebill_denial_detail.med_rec_num,
      prebill_denial_detail.birth_date,
      prebill_denial_detail.admit_date,
      prebill_denial_detail.discharge_date,
      prebill_denial_detail.provider_num_code,
      prebill_denial_detail.work_group_id,
      prebill_denial_detail.work_group_name,
      ROUND(prebill_denial_detail.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      ROUND(prebill_denial_detail.national_provider_id, 0, 'ROUND_HALF_EVEN') AS national_provider_id,
      prebill_denial_detail.iplan_ins1_id,
      prebill_denial_detail.iplan_id_ins1_name,
      prebill_denial_detail.iplan_ins2_id,
      prebill_denial_detail.iplan_id_ins2_name,
      prebill_denial_detail.pdu_initiated_date,
      prebill_denial_detail.status_code,
      prebill_denial_detail.status_desc,
      prebill_denial_detail.final_bill_date,
      prebill_denial_detail.bill_hold_start_date,
      prebill_denial_detail.bill_hold_end_date,
      prebill_denial_detail.xu_date,
      prebill_denial_detail.physician_advisor_vendor_name,
      prebill_denial_detail.denial_record_code,
      prebill_denial_detail.auth_code,
      prebill_denial_detail.auth_type_code,
      prebill_denial_detail.auth_date,
      prebill_denial_detail.initial_auth_status_code,
      prebill_denial_detail.current_deadline_date,
      prebill_denial_detail.certification_end_date,
      prebill_denial_detail.admitting_physician_name,
      prebill_denial_detail.attending_physician_name,
      ROUND(prebill_denial_detail.attending_physician_tax_id, 0, 'ROUND_HALF_EVEN') AS attending_physician_tax_id,
      prebill_denial_detail.service_code,
      prebill_denial_detail.admitting_dx_code,
      prebill_denial_detail.rebill_requested_date,
      prebill_denial_detail.review_reason1_desc,
      prebill_denial_detail.review_reason2_desc,
      prebill_denial_detail.review_reason3_desc,
      prebill_denial_detail.review_reason4_desc,
      prebill_denial_detail.denial_reason1_desc,
      prebill_denial_detail.denial_reason2_desc,
      prebill_denial_detail.denial_reason3_desc,
      prebill_denial_detail.denial_reason4_desc,
      prebill_denial_detail.root_cause1_desc,
      prebill_denial_detail.root_cause2_desc,
      prebill_denial_detail.root_cause3_desc,
      prebill_denial_detail.root_cause4_desc,
      prebill_denial_detail.pdu_iqreview1_ind,
      prebill_denial_detail.pdu_iqreview2_ind,
      prebill_denial_detail.pdu_iqreview3_ind,
      prebill_denial_detail.pdu_iqreview4_ind,
      prebill_denial_detail.agreed1_ind,
      prebill_denial_detail.agreed2_ind,
      prebill_denial_detail.agreed3_ind,
      prebill_denial_detail.agreed4_ind,
      prebill_denial_detail.sum_day_cnt,
      prebill_denial_detail.final_day_denied_cnt,
      ROUND(prebill_denial_detail.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(prebill_denial_detail.expected_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS expected_reimbursement_amt,
      ROUND(prebill_denial_detail.actual_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS actual_reimbursement_amt,
      prebill_denial_detail.determination_reason_code,
      prebill_denial_detail.determination_reason_desc,
      prebill_denial_detail.alos_day_cnt,
      prebill_denial_detail.no_certification_required_cnt,
      prebill_denial_detail.ssc_certed_day_cnt,
      prebill_denial_detail.cm_certed_day_cnt,
      prebill_denial_detail.post_discharge_day_cnt,
      prebill_denial_detail.inhouse_final_denied_day_cnt,
      prebill_denial_detail.post_discharge_overturned_day_cnt,
      prebill_denial_detail.denial_revenue_day1_cnt,
      prebill_denial_detail.denial_revenue_day2_cnt,
      prebill_denial_detail.denial_revenue_day3_cnt,
      prebill_denial_detail.denial_revenue_day4_cnt,
      prebill_denial_detail.total_approved_day_cnt,
      prebill_denial_detail.total_denied_day_cnt,
      prebill_denial_detail.ptp_outcome_desc,
      prebill_denial_detail.ptp_requested_date,
      prebill_denial_detail.final_case_closure_date,
      prebill_denial_detail.clinical_submitted_note_desc,
      prebill_denial_detail.clinical_submitted_date,
      prebill_denial_detail.payor_response_desc,
      prebill_denial_detail.denial_reconsideration_response,
      prebill_denial_detail.reconsideration_submit_date,
      prebill_denial_detail.interqual_completed_ind,
      prebill_denial_detail.analyst_reviewed_date,
      prebill_denial_detail.analyst_payor_contact_date,
      prebill_denial_detail.analyst_work_again_date,
      prebill_denial_detail.analyst_worked_user_id,
      prebill_denial_detail.nurse_initial_review_date,
      prebill_denial_detail.nurse_payor_contact_date,
      prebill_denial_detail.nurse_work_again_date,
      prebill_denial_detail.nurse_worked_user_id,
      prebill_denial_detail.denial_initial_review_date,
      prebill_denial_detail.denial_payor_contact_date,
      prebill_denial_detail.denial_work_again_date,
      prebill_denial_detail.denial_worked_user_id,
      prebill_denial_detail.verified_user_id,
      prebill_denial_detail.verification_date,
      prebill_denial_detail.modified_date,
      prebill_denial_detail.modified_user_id,
      prebill_denial_detail.created_date,
      prebill_denial_detail.created_user_id,
      prebill_denial_detail.assigned_user_id,
      prebill_denial_detail.manual_review_reason_desc,
      prebill_denial_detail.ar_status_code,
      prebill_denial_detail.denial_probability_desc,
      prebill_denial_detail.pilot_acct_ind,
      prebill_denial_detail.pilot_acct_user_review_1_txt,
      prebill_denial_detail.pilot_acct_user_review_2_txt,
      prebill_denial_detail.pilot_acct_user_review_3_txt,
      prebill_denial_detail.pilot_acct_user_review_4_txt
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.prebill_denial_detail
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/process_run_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.process_run_detail
   OPTIONS(description='This table has the information of start and completion of any EDWPBS ETL  Process. Used for Business facing for effective User Analysis')
  AS SELECT
      process_run_detail.process_name,
      process_run_detail.reporting_date,
      process_run_detail.table_name,
      process_run_detail.start_date_time,
      process_run_detail.end_date_time,
      process_run_detail.source_system_code,
      process_run_detail.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.process_run_detail
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/rcom_payor_dimension_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.rcom_payor_dimension_eom
   OPTIONS(description='Payor Dimension Table End of Month Snapshot having Parent Payor mappings')
  AS SELECT
      ROUND(rcom_payor_dimension_eom.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      rcom_payor_dimension_eom.pe_date,
      rcom_payor_dimension_eom.parent_payor_name,
      rcom_payor_dimension_eom.sub_payor_name,
      rcom_payor_dimension_eom.payor_short_name,
      rcom_payor_dimension_eom.product_name,
      rcom_payor_dimension_eom.contract_type_code,
      rcom_payor_dimension_eom.payor_id,
      rcom_payor_dimension_eom.eff_from_date,
      rcom_payor_dimension_eom.eff_to_date
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.rcom_payor_dimension_eom
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_ada_facility_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_ada_facility_metric
   OPTIONS(description='Reference Metrics table containing the percentages for Accrual calculations from ADA AR  Application for all Summary 7 facilities.')
  AS SELECT
      ref_ada_facility_metric.company_code,
      ref_ada_facility_metric.coid,
      ref_ada_facility_metric.eff_from_date,
      ref_ada_facility_metric.eff_to_date,
      ref_ada_facility_metric.unit_num,
      ROUND(ref_ada_facility_metric.ada_pct, 4, 'ROUND_HALF_EVEN') AS ada_pct,
      ROUND(ref_ada_facility_metric.secondary_pct, 4, 'ROUND_HALF_EVEN') AS secondary_pct,
      ROUND(ref_ada_facility_metric.charity_pct, 4, 'ROUND_HALF_EVEN') AS charity_pct,
      ROUND(ref_ada_facility_metric.uninsured_pct, 4, 'ROUND_HALF_EVEN') AS uninsured_pct,
      ROUND(ref_ada_facility_metric.spca_pct, 4, 'ROUND_HALF_EVEN') AS spca_pct,
      ref_ada_facility_metric.journal_entry_ind,
      ref_ada_facility_metric.source_system_code,
      ref_ada_facility_metric.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_ada_facility_metric
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_collection_claim_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_collection_claim_status
   OPTIONS(description='Table contains descriptions for all the collection Claim Status Codes present in the Artiva system')
  AS SELECT
      ref_collection_claim_status.claim_status_code,
      ref_collection_claim_status.claim_status_code_desc,
      ref_collection_claim_status.claim_status_relt_scat_id,
      ref_collection_claim_status.source_system_code,
      ref_collection_claim_status.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_collection_claim_status
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_collection_claim_status_scat.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_collection_claim_status_scat
   OPTIONS(description='Table contains related Sub category  descriptions for all the collection Claim Status Codes present in the Artiva system')
  AS SELECT
      ref_collection_claim_status_scat.claim_status_scat_id,
      ref_collection_claim_status_scat.claim_status_scat_desc,
      ref_collection_claim_status_scat.source_system_code,
      ref_collection_claim_status_scat.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_collection_claim_status_scat
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_collection_pool_assignment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_collection_pool_assignment
   OPTIONS(description='Table contains descriptions for all the collection pool identifiers present in the Artiva system')
  AS SELECT
      ref_collection_pool_assignment.pool_assignment_id,
      ref_collection_pool_assignment.artiva_instance_code,
      ref_collection_pool_assignment.pool_assignment_desc,
      ref_collection_pool_assignment.pool_category_desc,
      ref_collection_pool_assignment.active_ind,
      ref_collection_pool_assignment.dialer_name,
      ref_collection_pool_assignment.dialing_status_code,
      ref_collection_pool_assignment.inclusion_flag,
      ref_collection_pool_assignment.pool_assigned_location_name,
      ref_collection_pool_assignment.source_system_code,
      ref_collection_pool_assignment.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_collection_pool_assignment
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_collection_recall_reason.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_collection_recall_reason
   OPTIONS(description='Table contains related  Recall reason detail for all the accounts  recalled by the agency')
  AS SELECT
      ref_collection_recall_reason.recall_reason_code,
      ref_collection_recall_reason.recall_reason_desc,
      ref_collection_recall_reason.source_system_code,
      ref_collection_recall_reason.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_collection_recall_reason
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_mcaid_applied_program.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_mcaid_applied_program
   OPTIONS(description='Reference details of Applied Programs for Medicaid eligibility accounts.')
  AS SELECT
      ref_mcaid_applied_program.program_code,
      ref_mcaid_applied_program.program_desc,
      ref_mcaid_applied_program.source_system_code,
      ref_mcaid_applied_program.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_mcaid_applied_program
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_mcaid_elig_phase.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_mcaid_elig_phase
   OPTIONS(description='Reference table to describe Medicaid Eligibility Phases')
  AS SELECT
      ref_mcaid_elig_phase.mcaid_elig_phase_id,
      ref_mcaid_elig_phase.mcaid_elig_phase_desc,
      ref_mcaid_elig_phase.source_system_code,
      ref_mcaid_elig_phase.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_mcaid_elig_phase
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_mcaid_eligibility_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_mcaid_eligibility_status
   OPTIONS(description='Reference table with status details of Medicaid eligibility accounts.')
  AS SELECT
      ref_mcaid_eligibility_status.status_code,
      ref_mcaid_eligibility_status.status_desc,
      ref_mcaid_eligibility_status.source_system_code,
      ref_mcaid_eligibility_status.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_mcaid_eligibility_status
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_payor_spcl_report_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_payor_spcl_report_list
   OPTIONS(description='Reference table containing the list of reporting related instructions used in Payor Specialization Report List')
  AS SELECT
      ref_payor_spcl_report_list.report_list_sid,
      ref_payor_spcl_report_list.report_list_code,
      ref_payor_spcl_report_list.unit_num,
      ref_payor_spcl_report_list.report_parm_txt,
      ref_payor_spcl_report_list.business_owner_comment_txt,
      ref_payor_spcl_report_list.business_owner_name,
      ref_payor_spcl_report_list.iplan_id,
      ref_payor_spcl_report_list.group_id,
      ref_payor_spcl_report_list.procedure_code,
      ref_payor_spcl_report_list.revenue_code,
      ref_payor_spcl_report_list.federal_tax_id,
      ref_payor_spcl_report_list.ssc_coid,
      ref_payor_spcl_report_list.coid,
      ref_payor_spcl_report_list.company_code,
      ref_payor_spcl_report_list.diag_code,
      ref_payor_spcl_report_list.facility_state_code,
      ref_payor_spcl_report_list.eff_from_date,
      ref_payor_spcl_report_list.eff_to_date,
      ref_payor_spcl_report_list.source_system_code,
      ref_payor_spcl_report_list.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_payor_spcl_report_list
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_provider_service.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_provider_service
   OPTIONS(description='Reference table to maintain the Provider Service Identification Information of the Services recevied.')
  AS SELECT
      ROUND(ref_provider_service.remittance_provider_serv_sid, 0, 'ROUND_HALF_EVEN') AS remittance_provider_serv_sid,
      ref_provider_service.provider_serv_id_qlfr_code,
      ref_provider_service.provider_serv_id,
      ref_provider_service.source_system_code,
      ref_provider_service.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_provider_service
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_additional_payee.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_additional_payee
   OPTIONS(description='Reference table to maintain the Additional Payee details of the payments sent')
  AS SELECT
      ref_remittance_additional_payee.remittance_additional_payee_sid,
      ref_remittance_additional_payee.additional_payee_id_qualifier_code,
      ref_remittance_additional_payee.additional_payee_id,
      ref_remittance_additional_payee.source_system_code,
      ref_remittance_additional_payee.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_additional_payee
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_cob_carrier.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_cob_carrier
   OPTIONS(description='Reference table to maintain the Coordination Of Benefit Carrier details of the claims sent.')
  AS SELECT
      ref_remittance_cob_carrier.cob_carrier_sid,
      ref_remittance_cob_carrier.cob_qualifier_code,
      ref_remittance_cob_carrier.cob_carrier_num,
      ref_remittance_cob_carrier.cob_carrier_name,
      ref_remittance_cob_carrier.source_system_code,
      ref_remittance_cob_carrier.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_cob_carrier
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_corrected_priority_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_corrected_priority_payor
   OPTIONS(description='Reference table to maintain th eCorrected Priority Payor details of the claims sent.')
  AS SELECT
      ref_remittance_corrected_priority_payor.corrected_priority_payor_sid,
      ref_remittance_corrected_priority_payor.corrected_priority_payor_qualifier_code,
      ref_remittance_corrected_priority_payor.corrected_priority_payor_id,
      ref_remittance_corrected_priority_payor.corrected_priority_payor_name,
      ref_remittance_corrected_priority_payor.source_system_code,
      ref_remittance_corrected_priority_payor.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_corrected_priority_payor
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_oth_subscriber_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_oth_subscriber_info
   OPTIONS(description='Reference table to maintain the Other Subscriber details of the claims sent.')
  AS SELECT
      ref_remittance_oth_subscriber_info.remittance_oth_subscriber_sid,
      ref_remittance_oth_subscriber_info.oth_subscriber_id_qualifier_code,
      ref_remittance_oth_subscriber_info.oth_subscriber_id,
      ref_remittance_oth_subscriber_info.oth_subscriber_enty_type_qualifier_code,
      ref_remittance_oth_subscriber_info.oth_subscriber_last_name,
      ref_remittance_oth_subscriber_info.oth_subscriber_first_name,
      ref_remittance_oth_subscriber_info.oth_subscriber_middle_name,
      ref_remittance_oth_subscriber_info.oth_subscriber_name_suffix,
      ref_remittance_oth_subscriber_info.source_system_code,
      ref_remittance_oth_subscriber_info.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_oth_subscriber_info
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_other_claim_related_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_other_claim_related_info
   OPTIONS(description='Reference table to maintain the Other Claim Related Reference details of the Claims recevied.')
  AS SELECT
      ROUND(ref_remittance_other_claim_related_info.ref_sid, 0, 'ROUND_HALF_EVEN') AS ref_sid,
      ref_remittance_other_claim_related_info.reference_id_qualifier_code,
      ref_remittance_other_claim_related_info.reference_id,
      ref_remittance_other_claim_related_info.source_system_code,
      ref_remittance_other_claim_related_info.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_other_claim_related_info
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_payee.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_payee
   OPTIONS(description='Reference table to maintain the Payee details of the payments sent.')
  AS SELECT
      ref_remittance_payee.remittance_payee_sid,
      ref_remittance_payee.provider_npi,
      ref_remittance_payee.provider_tax_id,
      ref_remittance_payee.provider_tax_id_lookup_code,
      ref_remittance_payee.payee_name,
      ref_remittance_payee.payee_identification_qualifier_code,
      ref_remittance_payee.payee_city_name,
      ref_remittance_payee.payee_state_code,
      ref_remittance_payee.payee_postal_zone_code,
      ref_remittance_payee.source_system_code,
      ref_remittance_payee.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_payee
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_payor
   OPTIONS(description='Reference table to maintain the Payor details for the payments received.')
  AS SELECT
      ref_remittance_payor.remittance_payor_sid,
      ref_remittance_payor.payment_carrier_num,
      ref_remittance_payor.ep_payor_num,
      ref_remittance_payor.payment_agency_num_an,
      ref_remittance_payor.payor_ref_id,
      ref_remittance_payor.payor_name,
      ref_remittance_payor.payor_address_line_1,
      ref_remittance_payor.payor_address_line_2,
      ref_remittance_payor.payor_city_name,
      ref_remittance_payor.payor_state_code,
      ref_remittance_payor.payor_postal_zone_code,
      ref_remittance_payor.payor_line_of_business,
      ref_remittance_payor.payor_alternate_ref_id,
      ref_remittance_payor.payor_long_name,
      ref_remittance_payor.payor_short_name,
      ref_remittance_payor.payor_technical_contact_name,
      ref_remittance_payor.payor_primary_comm_type_code,
      ref_remittance_payor.payor_primary_contact_comm_num,
      ref_remittance_payor.payor_secondary_comm_type_code,
      ref_remittance_payor.payor_secondary_contact_comm_num,
      ref_remittance_payor.payor_tertiary_comm_type_code,
      ref_remittance_payor.payor_tertiary_contact_comm_num,
      ref_remittance_payor.source_system_code,
      ref_remittance_payor.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_payor
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_rendering_provider.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_rendering_provider
   OPTIONS(description='Reference table to maintain the Rendering Provider details of the claims sent.')
  AS SELECT
      ref_remittance_rendering_provider.remittance_rendering_provider_sid,
      ref_remittance_rendering_provider.serv_provider_enty_type_qualifier_code,
      ref_remittance_rendering_provider.rendering_provider_last_org_name,
      ref_remittance_rendering_provider.rendering_provider_first_name,
      ref_remittance_rendering_provider.rendering_provider_middle_name,
      ref_remittance_rendering_provider.rendering_provider_name_suffix,
      ref_remittance_rendering_provider.serv_provider_id_qualifier_code,
      ref_remittance_rendering_provider.rendering_provider_id,
      ref_remittance_rendering_provider.source_system_code,
      ref_remittance_rendering_provider.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_rendering_provider
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_subscriber_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_subscriber_info
   OPTIONS(description='Reference table to maintain the Subscriber details of the claims sent.')
  AS SELECT
      ref_remittance_subscriber_info.remittance_subscriber_sid,
      ref_remittance_subscriber_info.patient_health_ins_num,
      ref_remittance_subscriber_info.insured_identification_qualifier_code,
      ref_remittance_subscriber_info.subscriber_id,
      ref_remittance_subscriber_info.insured_entity_type_qualifier_code,
      ref_remittance_subscriber_info.subscriber_last_name,
      ref_remittance_subscriber_info.subscriber_first_name,
      ref_remittance_subscriber_info.subscriber_middle_name,
      ref_remittance_subscriber_info.subscriber_name_suffix,
      ref_remittance_subscriber_info.source_system_code,
      ref_remittance_subscriber_info.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_subscriber_info
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_request_file.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_request_file
   OPTIONS(description='Reference table to describe Unbilled Request File Type')
  AS SELECT
      ref_request_file.request_file_id,
      ref_request_file.file_name,
      ref_request_file.file_location_name,
      ref_request_file.source_system_code,
      ref_request_file.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_request_file
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_secn_remittance_rendering_provider.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_secn_remittance_rendering_provider
   OPTIONS(description='Reference table to maintain the Rendering Provider Secondary Information of the Claims recevied.')
  AS SELECT
      ROUND(ref_secn_remittance_rendering_provider.remittance_secn_rendering_provider_sid, 0, 'ROUND_HALF_EVEN') AS remittance_secn_rendering_provider_sid,
      ref_secn_remittance_rendering_provider.secn_rendering_provider_id_qlfr_code,
      ref_secn_remittance_rendering_provider.secn_rendering_provider_id,
      ref_secn_remittance_rendering_provider.source_system_code,
      ref_secn_remittance_rendering_provider.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_secn_remittance_rendering_provider
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_sr_vendor_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_sr_vendor_user
   OPTIONS(description='Reference table for Vendor Inventory that has all vendor information and the work stream status.')
  AS SELECT
      ref_sr_vendor_user.vendor_name,
      ref_sr_vendor_user.user_id,
      ref_sr_vendor_user.first_name,
      ref_sr_vendor_user.last_name,
      ref_sr_vendor_user.user_email_addr,
      ref_sr_vendor_user.job_role_text,
      ref_sr_vendor_user.request_type_text,
      ref_sr_vendor_user.request_status_text,
      ref_sr_vendor_user.location_name,
      ref_sr_vendor_user.workstream_name,
      ref_sr_vendor_user.source_system_code,
      ref_sr_vendor_user.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_sr_vendor_user
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_claim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.remittance_claim
   OPTIONS(description='This table contains claim level information associated with the payment.')
  AS SELECT
      remittance_claim.claim_guid,
      remittance_claim.payment_guid,
      ROUND(remittance_claim.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      remittance_claim.audit_date,
      remittance_claim.coid,
      remittance_claim.delete_ind,
      remittance_claim.delete_date,
      remittance_claim.unit_num,
      remittance_claim.company_code,
      remittance_claim.ssc_coid,
      remittance_claim.payor_patient_id,
      ROUND(remittance_claim.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      remittance_claim.patient_last_name,
      remittance_claim.patient_first_name,
      remittance_claim.patient_middle_initial,
      remittance_claim.mpi_ind,
      remittance_claim.iplan_id,
      remittance_claim.ep_calc_iplan_id,
      remittance_claim.iplan_insurance_order_num,
      remittance_claim.medical_record_num,
      remittance_claim.payer_claim_control_number,
      remittance_claim.stmt_cover_from_date,
      remittance_claim.stmt_cover_to_date,
      remittance_claim.received_date,
      remittance_claim.mpi_corrected_discharge_date,
      remittance_claim.patient_type_code_pos1,
      remittance_claim.financial_class_code,
      ROUND(remittance_claim.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(remittance_claim.mpi_calc_charge_amt, 3, 'ROUND_HALF_EVEN') AS mpi_calc_charge_amt,
      ROUND(remittance_claim.payment_amt, 3, 'ROUND_HALF_EVEN') AS payment_amt,
      ROUND(remittance_claim.ep_calc_denial_amount, 3, 'ROUND_HALF_EVEN') AS ep_calc_denial_amount,
      ROUND(remittance_claim.ep_calc_contractual_adj_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_contractual_adj_amt,
      ROUND(remittance_claim.mpi_contractual_adj_amt, 3, 'ROUND_HALF_EVEN') AS mpi_contractual_adj_amt,
      ROUND(remittance_claim.corrected_contractual_adj_amt, 3, 'ROUND_HALF_EVEN') AS corrected_contractual_adj_amt,
      ROUND(remittance_claim.net_benefit_amt, 3, 'ROUND_HALF_EVEN') AS net_benefit_amt,
      ROUND(remittance_claim.covered_charge_amt, 3, 'ROUND_HALF_EVEN') AS covered_charge_amt,
      ROUND(remittance_claim.mpi_calc_payor_previous_payment_amt, 3, 'ROUND_HALF_EVEN') AS mpi_calc_payor_previous_payment_amt,
      ROUND(remittance_claim.ep_calc_noncovered_charge_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_noncovered_charge_amt,
      ROUND(remittance_claim.mpi_calc_non_covered_charge_amt, 3, 'ROUND_HALF_EVEN') AS mpi_calc_non_covered_charge_amt,
      ROUND(remittance_claim.ep_calc_deductible_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_deductible_amt,
      ROUND(remittance_claim.mpi_calc_deductible_amt, 3, 'ROUND_HALF_EVEN') AS mpi_calc_deductible_amt,
      ROUND(remittance_claim.ep_calc_blood_deductible_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_blood_deductible_amt,
      ROUND(remittance_claim.ep_calc_coinsurance_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_coinsurance_amt,
      ROUND(remittance_claim.mpi_calc_coinsurance_amt, 3, 'ROUND_HALF_EVEN') AS mpi_calc_coinsurance_amt,
      ROUND(remittance_claim.ep_calc_primary_payor_payment_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_primary_payor_payment_amt,
      ROUND(remittance_claim.patient_liability_amount, 3, 'ROUND_HALF_EVEN') AS patient_liability_amount,
      ROUND(remittance_claim.capital_amt, 3, 'ROUND_HALF_EVEN') AS capital_amt,
      ROUND(remittance_claim.ep_calc_discount_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_discount_amt,
      ROUND(remittance_claim.disproportionate_share_amt, 3, 'ROUND_HALF_EVEN') AS disproportionate_share_amt,
      ROUND(remittance_claim.drg_disproportionate_share_amt, 3, 'ROUND_HALF_EVEN') AS drg_disproportionate_share_amt,
      ROUND(remittance_claim.drg_amt, 3, 'ROUND_HALF_EVEN') AS drg_amt,
      remittance_claim.drg_code,
      remittance_claim.mpi_calc_drg_code,
      remittance_claim.mpi_calc_drg_grouper_code,
      ROUND(remittance_claim.federal_specific_drg_amt, 3, 'ROUND_HALF_EVEN') AS federal_specific_drg_amt,
      ROUND(remittance_claim.hospital_specific_drg_amt, 3, 'ROUND_HALF_EVEN') AS hospital_specific_drg_amt,
      ROUND(remittance_claim.drg_weight_amt, 5, 'ROUND_HALF_EVEN') AS drg_weight_amt,
      ROUND(remittance_claim.hcpcs_charge_amt, 3, 'ROUND_HALF_EVEN') AS hcpcs_charge_amt,
      ROUND(remittance_claim.hcpcs_payment_amt, 3, 'ROUND_HALF_EVEN') AS hcpcs_payment_amt,
      ROUND(remittance_claim.indirect_medical_education_amt, 3, 'ROUND_HALF_EVEN') AS indirect_medical_education_amt,
      ROUND(remittance_claim.indirect_teaching_amt, 3, 'ROUND_HALF_EVEN') AS indirect_teaching_amt,
      ROUND(remittance_claim.interest_amt, 3, 'ROUND_HALF_EVEN') AS interest_amt,
      ROUND(remittance_claim.ep_calc_lab_charge_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_lab_charge_amt,
      ROUND(remittance_claim.ep_calc_lab_payment_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_lab_payment_amt,
      ROUND(remittance_claim.pps_opr_federal_specific_drg_amt, 3, 'ROUND_HALF_EVEN') AS pps_opr_federal_specific_drg_amt,
      ROUND(remittance_claim.mpi_calc_non_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS mpi_calc_non_billed_charge_amt,
      ROUND(remittance_claim.mpi_calc_net_benefit_amt, 3, 'ROUND_HALF_EVEN') AS mpi_calc_net_benefit_amt,
      ROUND(remittance_claim.ep_calc_non_payable_professional_fee_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_non_payable_professional_fee_amt,
      ROUND(remittance_claim.old_capital_amt, 3, 'ROUND_HALF_EVEN') AS old_capital_amt,
      ROUND(remittance_claim.operating_outlier_amt, 3, 'ROUND_HALF_EVEN') AS operating_outlier_amt,
      ROUND(remittance_claim.outlier_amt, 3, 'ROUND_HALF_EVEN') AS outlier_amt,
      ROUND(remittance_claim.outpatient_reimibursement_rate_amt, 3, 'ROUND_HALF_EVEN') AS outpatient_reimibursement_rate_amt,
      ROUND(remittance_claim.ep_calc_therapy_charge_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_therapy_charge_amt,
      ROUND(remittance_claim.ep_calc_therapy_payment_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_therapy_payment_amt,
      ROUND(remittance_claim.pps_capital_outlier_amt, 3, 'ROUND_HALF_EVEN') AS pps_capital_outlier_amt,
      ROUND(remittance_claim.ins_covered_day_cnt, 2, 'ROUND_HALF_EVEN') AS ins_covered_day_cnt,
      remittance_claim.ep_calc_covered_day_cnt,
      remittance_claim.noncovered_day_cnt,
      remittance_claim.cost_report_day_cnt,
      ROUND(remittance_claim.discharge_fraction_pct, 4, 'ROUND_HALF_EVEN') AS discharge_fraction_pct,
      remittance_claim.lifetime_psychiatric_day_cnt,
      remittance_claim.contractual_adj_amt_procedure_code,
      remittance_claim.payment_amt_procedure_code,
      remittance_claim.logging_flag,
      remittance_claim.logging_type_code,
      remittance_claim.mpi_calc_interim_bill_ind,
      remittance_claim.claim_status_code,
      remittance_claim.ep_calc_status_code,
      remittance_claim.ep_calc_denial_code_ind,
      remittance_claim.mpi_calc_denial_code_ind,
      remittance_claim.type_of_claim_code,
      remittance_claim.type_of_bill_code,
      remittance_claim.ep_calc_balanced_ind,
      remittance_claim.discount_applied_ind,
      remittance_claim.ep_calc_plb_opay_rcvy_trx_ind,
      remittance_claim.ep_calc_internal_denial_code,
      remittance_claim.concuity_acct_ind,
      remittance_claim.mia_switch_ind,
      remittance_claim.moa_switch_ind,
      ROUND(remittance_claim.mia_pps_capital_exception_amt, 3, 'ROUND_HALF_EVEN') AS mia_pps_capital_exception_amt,
      remittance_claim.casemix_ind,
      remittance_claim.zero_default_iplan_ind,
      remittance_claim.trx_tracking_not_released_ind,
      remittance_claim.qmb_switch_ind,
      remittance_claim.ep_calc_denial_code_1,
      remittance_claim.ep_calc_denial_code_2,
      remittance_claim.ep_calc_denial_code_3,
      remittance_claim.ep_calc_denial_code_4,
      remittance_claim.ep_calc_denial_code_5,
      remittance_claim.ep_calc_denial_code_6,
      remittance_claim.ep_calc_denial_code_7,
      remittance_claim.ep_calc_denial_code_8,
      remittance_claim.ep_calc_denial_code_9,
      remittance_claim.ep_calc_denial_code_10,
      remittance_claim.ep_calc_handling_ind,
      remittance_claim.crossover_payor_name,
      remittance_claim.ep_calc_payor_category_code,
      remittance_claim.secondary_payor_flag,
      remittance_claim.ep_calc_prim_itnl_pyr_code,
      remittance_claim.ep_calc_secn_itnl_pyr_code,
      remittance_claim.ep_calc_tert_itnl_pyr_code,
      remittance_claim.corrected_priority_payor_sid,
      remittance_claim.cob_carrier_sid,
      remittance_claim.corrected_subscriber_last_name,
      remittance_claim.corrected_subscriber_first_name,
      remittance_claim.corrected_subscriber_middle_name,
      remittance_claim.corrected_subscriber_health_ins_num,
      remittance_claim.remittance_subscriber_sid,
      remittance_claim.remittance_oth_subscriber_sid,
      remittance_claim.remittance_rendering_provider_sid,
      remittance_claim.supplemental_amt_qlfr_code_1,
      ROUND(remittance_claim.supplemental_amt_1, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_1,
      remittance_claim.supplemental_amt_qlfr_code_2,
      ROUND(remittance_claim.supplemental_amt_2, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_2,
      remittance_claim.supplemental_amt_qlfr_code_3,
      ROUND(remittance_claim.supplemental_amt_3, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_3,
      remittance_claim.supplemental_amt_qlfr_code_4,
      ROUND(remittance_claim.supplemental_amt_4, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_4,
      remittance_claim.supplemental_amt_qlfr_code_5,
      ROUND(remittance_claim.supplemental_amt_5, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_5,
      remittance_claim.supplemental_amt_qlfr_code_6,
      ROUND(remittance_claim.supplemental_amt_6, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_6,
      remittance_claim.supplemental_amt_qlfr_code_7,
      ROUND(remittance_claim.supplemental_amt_7, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_7,
      remittance_claim.supplemental_amt_qlfr_code_8,
      ROUND(remittance_claim.supplemental_amt_8, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_8,
      remittance_claim.supplemental_amt_qlfr_code_9,
      ROUND(remittance_claim.supplemental_amt_9, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_9,
      remittance_claim.supplemental_amt_qlfr_code_10,
      ROUND(remittance_claim.supplemental_amt_10, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_10,
      remittance_claim.supplemental_amt_qlfr_code_11,
      ROUND(remittance_claim.supplemental_amt_11, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_11,
      remittance_claim.supplemental_amt_qlfr_code_12,
      ROUND(remittance_claim.supplemental_amt_12, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_12,
      remittance_claim.supplemental_amt_qlfr_code_13,
      ROUND(remittance_claim.supplemental_amt_13, 3, 'ROUND_HALF_EVEN') AS supplemental_amt_13,
      remittance_claim.supplemental_qty_qlfr_code_1,
      ROUND(remittance_claim.supplemental_qty_1, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_1,
      remittance_claim.supplemental_qty_qlfr_code_2,
      ROUND(remittance_claim.supplemental_qty_2, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_2,
      remittance_claim.supplemental_qty_qlfr_code_3,
      ROUND(remittance_claim.supplemental_qty_3, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_3,
      remittance_claim.supplemental_qty_qlfr_code_4,
      ROUND(remittance_claim.supplemental_qty_4, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_4,
      remittance_claim.supplemental_qty_qlfr_code_5,
      ROUND(remittance_claim.supplemental_qty_5, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_5,
      remittance_claim.supplemental_qty_qlfr_code_6,
      ROUND(remittance_claim.supplemental_qty_6, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_6,
      remittance_claim.supplemental_qty_qlfr_code_7,
      ROUND(remittance_claim.supplemental_qty_7, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_7,
      remittance_claim.supplemental_qty_qlfr_code_8,
      ROUND(remittance_claim.supplemental_qty_8, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_8,
      remittance_claim.supplemental_qty_qlfr_code_9,
      ROUND(remittance_claim.supplemental_qty_9, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_9,
      remittance_claim.supplemental_qty_qlfr_code_10,
      ROUND(remittance_claim.supplemental_qty_10, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_10,
      remittance_claim.supplemental_qty_qlfr_code_11,
      ROUND(remittance_claim.supplemental_qty_11, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_11,
      remittance_claim.supplemental_qty_qlfr_code_12,
      ROUND(remittance_claim.supplemental_qty_12, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_12,
      remittance_claim.supplemental_qty_qlfr_code_13,
      ROUND(remittance_claim.supplemental_qty_13, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_13,
      remittance_claim.supplemental_qty_qlfr_code_14,
      ROUND(remittance_claim.supplemental_qty_14, 0, 'ROUND_HALF_EVEN') AS supplemental_qty_14,
      remittance_claim.rarc_code_1,
      remittance_claim.rarc_code_2,
      remittance_claim.rarc_code_3,
      remittance_claim.rarc_code_4,
      remittance_claim.rarc_code_5,
      remittance_claim.source_system_code,
      remittance_claim.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.remittance_claim
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_claim_carc.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.remittance_claim_carc
   OPTIONS(description='This is the information related to claim level adjustments associated with the claim.')
  AS SELECT
      remittance_claim_carc.claim_guid,
      remittance_claim_carc.adj_group_code,
      remittance_claim_carc.carc_code,
      remittance_claim_carc.audit_date,
      remittance_claim_carc.delete_ind,
      remittance_claim_carc.delete_date,
      remittance_claim_carc.coid,
      remittance_claim_carc.company_code,
      ROUND(remittance_claim_carc.adj_amt, 3, 'ROUND_HALF_EVEN') AS adj_amt,
      remittance_claim_carc.adj_qty,
      remittance_claim_carc.adj_category,
      remittance_claim_carc.cc_adj_group_code,
      remittance_claim_carc.dw_last_update_date_time,
      remittance_claim_carc.source_system_code
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.remittance_claim_carc
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_payment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.remittance_payment
   OPTIONS(description='This is the check payment information recevied by Electronic Payment team from each payor by facility/provider.It also includes Control elements associated with the payer')
  AS SELECT
      remittance_payment.payment_guid,
      remittance_payment.check_num_an,
      remittance_payment.check_date,
      ROUND(remittance_payment.check_amt, 3, 'ROUND_HALF_EVEN') AS check_amt,
      remittance_payment.interchange_sender_id,
      remittance_payment.provider_level_adj_id,
      remittance_payment.remittance_payor_sid,
      remittance_payment.remittance_payee_sid,
      remittance_payment.audit_date,
      remittance_payment.delete_ind,
      remittance_payment.delete_date,
      remittance_payment.coid,
      remittance_payment.company_code,
      remittance_payment.unit_num,
      remittance_payment.era_create_date,
      remittance_payment.bill_process_date,
      remittance_payment.ep_effective_post_date,
      remittance_payment.ep_create_date,
      ROUND(remittance_payment.bill_charge_amt, 3, 'ROUND_HALF_EVEN') AS bill_charge_amt,
      ROUND(remittance_payment.covered_charge_amt, 3, 'ROUND_HALF_EVEN') AS covered_charge_amt,
      ROUND(remittance_payment.ep_non_covered_charge_amt, 3, 'ROUND_HALF_EVEN') AS ep_non_covered_charge_amt,
      ROUND(remittance_payment.ep_coinsurance_amt, 3, 'ROUND_HALF_EVEN') AS ep_coinsurance_amt,
      ROUND(remittance_payment.ep_contractual_adj_amt, 3, 'ROUND_HALF_EVEN') AS ep_contractual_adj_amt,
      ROUND(remittance_payment.ep_deductible_amt, 3, 'ROUND_HALF_EVEN') AS ep_deductible_amt,
      ROUND(remittance_payment.ep_denial_amt, 3, 'ROUND_HALF_EVEN') AS ep_denial_amt,
      ROUND(remittance_payment.drg_amt, 3, 'ROUND_HALF_EVEN') AS drg_amt,
      ROUND(remittance_payment.federal_specific_drg_amt, 3, 'ROUND_HALF_EVEN') AS federal_specific_drg_amt,
      ROUND(remittance_payment.ep_calc_blood_deductible_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_blood_deductible_amt,
      ROUND(remittance_payment.ep_calc_hcpcs_charge_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_hcpcs_charge_amt,
      ROUND(remittance_payment.ep_calc_hcpcs_payment_amt, 3, 'ROUND_HALF_EVEN') AS ep_calc_hcpcs_payment_amt,
      ROUND(remittance_payment.apc_amt, 3, 'ROUND_HALF_EVEN') AS apc_amt,
      ROUND(remittance_payment.capital_amt, 3, 'ROUND_HALF_EVEN') AS capital_amt,
      ROUND(remittance_payment.disproportionate_share_amt, 3, 'ROUND_HALF_EVEN') AS disproportionate_share_amt,
      ROUND(remittance_payment.indirect_teaching_amt, 3, 'ROUND_HALF_EVEN') AS indirect_teaching_amt,
      ROUND(remittance_payment.interest_amt, 3, 'ROUND_HALF_EVEN') AS interest_amt,
      ROUND(remittance_payment.claim_payment_amt, 3, 'ROUND_HALF_EVEN') AS claim_payment_amt,
      ROUND(remittance_payment.ep_lab_charge_amt, 3, 'ROUND_HALF_EVEN') AS ep_lab_charge_amt,
      ROUND(remittance_payment.ep_lab_payment_amt, 3, 'ROUND_HALF_EVEN') AS ep_lab_payment_amt,
      ROUND(remittance_payment.ep_nonpayable_professional_fee_amt, 3, 'ROUND_HALF_EVEN') AS ep_nonpayable_professional_fee_amt,
      ROUND(remittance_payment.outlier_amt, 3, 'ROUND_HALF_EVEN') AS outlier_amt,
      ROUND(remittance_payment.patient_liability_amt, 3, 'ROUND_HALF_EVEN') AS patient_liability_amt,
      ROUND(remittance_payment.ep_primary_insurance_payment_amt, 3, 'ROUND_HALF_EVEN') AS ep_primary_insurance_payment_amt,
      ROUND(remittance_payment.ep_therapy_charge_amt, 3, 'ROUND_HALF_EVEN') AS ep_therapy_charge_amt,
      ROUND(remittance_payment.ep_therapy_payment_amt, 3, 'ROUND_HALF_EVEN') AS ep_therapy_payment_amt,
      ROUND(remittance_payment.ins_covered_day_cnt, 2, 'ROUND_HALF_EVEN') AS ins_covered_day_cnt,
      remittance_payment.ep_calc_covered_day_cnt,
      remittance_payment.noncovered_day_cnt,
      remittance_payment.claim_cnt,
      remittance_payment.ep_discharge_cnt,
      remittance_payment.cost_report_day_cnt,
      remittance_payment.remittance_seq_num,
      remittance_payment.ep_plb_key_num,
      ROUND(remittance_payment.boa_lockbox_num, 0, 'ROUND_HALF_EVEN') AS boa_lockbox_num,
      remittance_payment.interchange_receiver_id,
      remittance_payment.interchange_sender_qualifier_code,
      remittance_payment.interchange_sender_code,
      remittance_payment.interchange_receiver_id_qualifier_code,
      remittance_payment.interchange_receiver_code,
      remittance_payment.transmission_sending_code,
      remittance_payment.transmission_receiving_code,
      remittance_payment.bill_type_code,
      remittance_payment.payor_default_logging_type_code,
      remittance_payment.ep_payor_batch_code,
      remittance_payment.rac_ind,
      remittance_payment.payor_live_ind,
      remittance_payment.lab_transaction_breakout_ind,
      remittance_payment.comment_transaction_ind,
      remittance_payment.internal_denial_transaction_ind,
      remittance_payment.discharge_date_replacement_ind,
      remittance_payment.total_charges_replacement_ind,
      remittance_payment.patient_type_replacement_ind,
      remittance_payment.drg_replacement_ind,
      remittance_payment.apply_secondary_rule_ind,
      remittance_payment.apply_recover_rule_ind,
      remittance_payment.apply_mother_baby_rule_ind,
      remittance_payment.apply_total_charges_rule_ind,
      remittance_payment.apply_noncovered_charges_rule_ind,
      remittance_payment.apply_er_patient_rule_ind,
      remittance_payment.apply_recover_true_up_rule_ind,
      remittance_payment.apply_non_recover_true_up_rule_ind,
      remittance_payment.calculated_covered_days_ind,
      remittance_payment.alternate_sn_format_ind,
      remittance_payment.plb_recovery_transaction_ind,
      remittance_payment.duplicate_direct_payer_ind,
      remittance_payment.not_post_payer_ind,
      remittance_payment.source_system_code,
      remittance_payment.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.remittance_payment
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_plb_adj.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.remittance_plb_adj
   OPTIONS(description='This table contains information related to Provider level Balance adjustments associated with the payment checks.')
  AS SELECT
      remittance_plb_adj.payment_guid,
      remittance_plb_adj.audit_date,
      remittance_plb_adj.adj_reason_code,
      remittance_plb_adj.adj_ref_id,
      remittance_plb_adj.adj_record_num,
      remittance_plb_adj.delete_ind,
      remittance_plb_adj.delete_date,
      remittance_plb_adj.unit_num,
      remittance_plb_adj.iplan_id,
      remittance_plb_adj.procedure_code,
      ROUND(remittance_plb_adj.adj_amt, 3, 'ROUND_HALF_EVEN') AS adj_amt,
      remittance_plb_adj.adj_match_code,
      remittance_plb_adj.payer_claim_control_number,
      remittance_plb_adj.ep_calc_claim_control_num,
      remittance_plb_adj.discharge_date,
      remittance_plb_adj.fiscal_period_date,
      remittance_plb_adj.patient_adj_ind,
      remittance_plb_adj.general_ledger_adj_ind,
      remittance_plb_adj.source_system_code,
      remittance_plb_adj.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.remittance_plb_adj
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_service.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.remittance_service
   OPTIONS(description='This table has all the services provided for the patient associated to a payment and claim.')
  AS SELECT
      remittance_service.service_guid,
      remittance_service.claim_guid,
      remittance_service.audit_date,
      remittance_service.coid,
      remittance_service.company_code,
      remittance_service.delete_ind,
      remittance_service.delete_date,
      ROUND(remittance_service.charge_amt, 3, 'ROUND_HALF_EVEN') AS charge_amt,
      ROUND(remittance_service.payment_amt, 3, 'ROUND_HALF_EVEN') AS payment_amt,
      ROUND(remittance_service.coinsurance_amt, 3, 'ROUND_HALF_EVEN') AS coinsurance_amt,
      ROUND(remittance_service.deductible_amt, 3, 'ROUND_HALF_EVEN') AS deductible_amt,
      remittance_service.adjudicated_hcpcs_code,
      remittance_service.submitted_hcpcs_code,
      remittance_service.submitted_hcpcs_code_desc,
      remittance_service.payor_sent_revenue_code,
      remittance_service.adjudicated_hipps_code,
      remittance_service.submitted_hipps_code,
      remittance_service.apc_code,
      ROUND(remittance_service.apc_amt, 3, 'ROUND_HALF_EVEN') AS apc_amt,
      ROUND(remittance_service.adjudicated_service_qty, 2, 'ROUND_HALF_EVEN') AS adjudicated_service_qty,
      ROUND(remittance_service.submitted_service_qty, 2, 'ROUND_HALF_EVEN') AS submitted_service_qty,
      remittance_service.service_category_code,
      remittance_service.date_time_qualifier_code_1,
      remittance_service.service_date_1,
      remittance_service.date_time_qualifier_code_2,
      remittance_service.service_date_2,
      remittance_service.dw_last_update_date_time,
      remittance_service.source_system_code
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.remittance_service
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_service_carc.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.remittance_service_carc
   OPTIONS(description='This is the information related to Service level adjustments associated with the service.')
  AS SELECT
      remittance_service_carc.service_guid,
      remittance_service_carc.adj_group_code,
      remittance_service_carc.carc_code,
      remittance_service_carc.audit_date,
      remittance_service_carc.delete_ind,
      remittance_service_carc.delete_date,
      remittance_service_carc.coid,
      remittance_service_carc.company_code,
      ROUND(remittance_service_carc.adj_amt, 3, 'ROUND_HALF_EVEN') AS adj_amt,
      remittance_service_carc.adj_qty,
      remittance_service_carc.adj_category,
      remittance_service_carc.cc_adj_group_code,
      remittance_service_carc.dw_last_update_date_time,
      remittance_service_carc.source_system_code
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.remittance_service_carc
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_service_hcpcs_modifier.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.remittance_service_hcpcs_modifier
   OPTIONS(description='Table to maintain the Service Procedure Modifiers which identifies special circumstances related to the performance of the service')
  AS SELECT
      remittance_service_hcpcs_modifier.service_guid,
      remittance_service_hcpcs_modifier.hcpcs_type_ind,
      remittance_service_hcpcs_modifier.hcpcs_modifier_seq_num,
      remittance_service_hcpcs_modifier.hcpcs_modifier_code,
      remittance_service_hcpcs_modifier.source_system_code,
      remittance_service_hcpcs_modifier.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.remittance_service_hcpcs_modifier
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/remittance_service_rarc.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.remittance_service_rarc
   OPTIONS(description='This is the service level remarks associated with the Service.')
  AS SELECT
      remittance_service_rarc.service_guid,
      remittance_service_rarc.rarc_qualifier_code,
      remittance_service_rarc.rarc_code,
      remittance_service_rarc.audit_date,
      remittance_service_rarc.delete_ind,
      remittance_service_rarc.delete_date,
      remittance_service_rarc.coid,
      remittance_service_rarc.company_code,
      remittance_service_rarc.dw_last_update_date_time,
      remittance_service_rarc.source_system_code
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.remittance_service_rarc
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/rpt_ar_ada_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.rpt_ar_ada_detail
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
      `hca-hin-curated-mirroring-td`.edwpbs.rpt_ar_ada_detail
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/sma_rate_calculation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.sma_rate_calculation
   OPTIONS(description='Every month this table maitains the Threshold Amounts and the payment rate calculations for all the patients across a facility.')
  AS SELECT
      sma_rate_calculation.month_id,
      sma_rate_calculation.coid,
      sma_rate_calculation.company_code,
      sma_rate_calculation.patient_type_code,
      ROUND(sma_rate_calculation.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      sma_rate_calculation.iplan_id,
      sma_rate_calculation.unit_num,
      sma_rate_calculation.case_cnt,
      ROUND(sma_rate_calculation.sma_threshold_amt, 3, 'ROUND_HALF_EVEN') AS sma_threshold_amt,
      ROUND(sma_rate_calculation.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
      ROUND(sma_rate_calculation.total_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_payment_amt,
      ROUND(sma_rate_calculation.financial_class_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS financial_class_payment_rate_calc,
      ROUND(sma_rate_calculation.iplan_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS iplan_payment_rate_calc,
      ROUND(sma_rate_calculation.sma_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS sma_payment_rate_calc,
      sma_rate_calculation.source_system_code,
      sma_rate_calculation.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.sma_rate_calculation
  ;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_ar_cash_collections_lp.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.smry_ar_cash_collections_lp AS SELECT
    smry_ar_cash_collections_lp.rptg_period,
    smry_ar_cash_collections_lp.month_num,
    smry_ar_cash_collections_lp.week_of_month,
    smry_ar_cash_collections_lp.ar_transaction_enter_date,
    smry_ar_cash_collections_lp.company_code,
    smry_ar_cash_collections_lp.coid,
    smry_ar_cash_collections_lp.unit_num,
    smry_ar_cash_collections_lp.derived_patient_type_code,
    ROUND(smry_ar_cash_collections_lp.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
    ROUND(smry_ar_cash_collections_lp.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    smry_ar_cash_collections_lp.parent_payor_name,
    smry_ar_cash_collections_lp.payor_id,
    smry_ar_cash_collections_lp.iplan_id,
    smry_ar_cash_collections_lp.source_system_code,
    ROUND(smry_ar_cash_collections_lp.cash_amt, 3, 'ROUND_HALF_EVEN') AS cash_amt,
    ROUND(smry_ar_cash_collections_lp.adjusted_net_revenue_amt, 3, 'ROUND_HALF_EVEN') AS adjusted_net_revenue_amt,
    smry_ar_cash_collections_lp.up_front_collection_ind
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.smry_ar_cash_collections_lp
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_ar_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.smry_ar_metric AS SELECT
    smry_ar_metric.service_type_name,
    smry_ar_metric.fact_lvl_code,
    smry_ar_metric.parent_code,
    smry_ar_metric.child_code,
    smry_ar_metric.ytd_month_ind,
    smry_ar_metric.month_id,
    smry_ar_metric.patient_type_desc,
    ROUND(smry_ar_metric.bad_debt_wo_amt, 3, 'ROUND_HALF_EVEN') AS bad_debt_wo_amt,
    ROUND(smry_ar_metric.total_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_balance_amt,
    ROUND(smry_ar_metric.net_revenue_amt, 3, 'ROUND_HALF_EVEN') AS net_revenue_amt,
    ROUND(smry_ar_metric.medicare_ar_day_cnt, 3, 'ROUND_HALF_EVEN') AS medicare_ar_day_cnt,
    ROUND(smry_ar_metric.mc_fc9_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS mc_fc9_total_charge_amt,
    ROUND(smry_ar_metric.mcaid_fc3_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS mcaid_fc3_total_charge_amt,
    ROUND(smry_ar_metric.mcaid_conv_ttl_charge_amt, 3, 'ROUND_HALF_EVEN') AS mcaid_conv_ttl_charge_amt,
    ROUND(smry_ar_metric.self_pay_3_mth_revenue_avg, 3, 'ROUND_HALF_EVEN') AS self_pay_3_mth_revenue_avg,
    smry_ar_metric.dw_last_update_date_time,
    smry_ar_metric.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.smry_ar_metric
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_ar_metric_other.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.smry_ar_metric_other AS SELECT
    smry_ar_metric_other.service_type_name,
    smry_ar_metric_other.fact_lvl_code,
    smry_ar_metric_other.parent_code,
    smry_ar_metric_other.child_code,
    smry_ar_metric_other.ytd_month_ind,
    smry_ar_metric_other.month_id,
    smry_ar_metric_other.payor_type,
    smry_ar_metric_other.payor_short_name,
    smry_ar_metric_other.age_group_desc,
    smry_ar_metric_other.payor_fin_class_desc,
    smry_ar_metric_other.account_type_desc,
    smry_ar_metric_other.age_group_member,
    smry_ar_metric_other.scenario_type,
    ROUND(smry_ar_metric_other.ar_bal_amt, 3, 'ROUND_HALF_EVEN') AS ar_bal_amt,
    ROUND(smry_ar_metric_other.total_ins_amt, 3, 'ROUND_HALF_EVEN') AS total_ins_amt,
    ROUND(smry_ar_metric_other.adj_net_rev_amt, 3, 'ROUND_HALF_EVEN') AS adj_net_rev_amt,
    ROUND(smry_ar_metric_other.charity_amt, 3, 'ROUND_HALF_EVEN') AS charity_amt,
    ROUND(smry_ar_metric_other.gross_revenue_amt, 3, 'ROUND_HALF_EVEN') AS gross_revenue_amt,
    ROUND(smry_ar_metric_other.ins_gt_90_day_amt, 3, 'ROUND_HALF_EVEN') AS ins_gt_90_day_amt,
    ROUND(smry_ar_metric_other.ins_gt_90_day_goal_amt, 3, 'ROUND_HALF_EVEN') AS ins_gt_90_day_goal_amt,
    ROUND(smry_ar_metric_other.uninsured_amt, 3, 'ROUND_HALF_EVEN') AS uninsured_amt,
    ROUND(smry_ar_metric_other.bad_debt_amt, 3, 'ROUND_HALF_EVEN') AS bad_debt_amt,
    ROUND(smry_ar_metric_other.net_ar_amt, 3, 'ROUND_HALF_EVEN') AS net_ar_amt,
    ROUND(smry_ar_metric_other.clearing_acct_amt, 3, 'ROUND_HALF_EVEN') AS clearing_acct_amt,
    smry_ar_metric_other.days_in_month,
    smry_ar_metric_other.dw_last_update_date_time,
    smry_ar_metric_other.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.smry_ar_metric_other
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_cash_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.smry_cash_metric AS SELECT
    smry_cash_metric.service_type_name,
    smry_cash_metric.fact_lvl_code,
    smry_cash_metric.parent_code,
    smry_cash_metric.child_code,
    smry_cash_metric.ytd_month_ind,
    smry_cash_metric.month_id,
    smry_cash_metric.payor_short_name,
    smry_cash_metric.ufc_pmt_type,
    smry_cash_metric.patient_type,
    ROUND(smry_cash_metric.cash_amt, 3, 'ROUND_HALF_EVEN') AS cash_amt,
    ROUND(smry_cash_metric.fm_cash_amt, 3, 'ROUND_HALF_EVEN') AS fm_cash_amt,
    ROUND(smry_cash_metric.adj_net_rev_amt, 3, 'ROUND_HALF_EVEN') AS adj_net_rev_amt,
    ROUND(smry_cash_metric.upfront_coll_amt, 3, 'ROUND_HALF_EVEN') AS upfront_coll_amt,
    ROUND(smry_cash_metric.net_revenue_amt, 3, 'ROUND_HALF_EVEN') AS net_revenue_amt,
    ROUND(smry_cash_metric.net_revenue_2m_prior_amt, 3, 'ROUND_HALF_EVEN') AS net_revenue_2m_prior_amt,
    ROUND(smry_cash_metric.gross_revenue_amt, 3, 'ROUND_HALF_EVEN') AS gross_revenue_amt,
    ROUND(smry_cash_metric.gt_smry_day_bus_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS gt_smry_day_bus_acct_bal_amt,
    ROUND(smry_cash_metric.gt_smry_day_med_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS gt_smry_day_med_acct_bal_amt,
    ROUND(smry_cash_metric.total_ssi_amt, 3, 'ROUND_HALF_EVEN') AS total_ssi_amt,
    smry_cash_metric.bank_days,
    smry_cash_metric.days_in_month,
    smry_cash_metric.dw_last_update_date_time,
    smry_cash_metric.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.smry_cash_metric
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_medicaid_conv_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.smry_medicaid_conv_rate AS SELECT
    smry_medicaid_conv_rate.coid,
    smry_medicaid_conv_rate.company_code,
    smry_medicaid_conv_rate.month_id,
    smry_medicaid_conv_rate.patient_type_desc,
    ROUND(smry_medicaid_conv_rate.mc_fc9_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS mc_fc9_total_charge_amt,
    ROUND(smry_medicaid_conv_rate.mcaid_fc3_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS mcaid_fc3_total_charge_amt,
    ROUND(smry_medicaid_conv_rate.mcaid_conv_ttl_charge_amt, 3, 'ROUND_HALF_EVEN') AS mcaid_conv_ttl_charge_amt,
    ROUND(smry_medicaid_conv_rate.self_pay_3_mth_revenue_avg, 3, 'ROUND_HALF_EVEN') AS self_pay_3_mth_revenue_avg,
    smry_medicaid_conv_rate.dw_last_update_date_time,
    smry_medicaid_conv_rate.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.smry_medicaid_conv_rate
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_pc_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.smry_pc_metric AS SELECT
    smry_pc_metric.service_type_name,
    smry_pc_metric.fact_lvl_code,
    smry_pc_metric.parent_code,
    smry_pc_metric.child_code,
    smry_pc_metric.ytd_month_ind,
    smry_pc_metric.month_id,
    smry_pc_metric.payor_short_name,
    smry_pc_metric.denial_short_desc,
    ROUND(smry_pc_metric.unpay_inv_amt, 3, 'ROUND_HALF_EVEN') AS unpay_inv_amt,
    ROUND(smry_pc_metric.unpay_reason_inv_amt, 3, 'ROUND_HALF_EVEN') AS unpay_reason_inv_amt,
    ROUND(smry_pc_metric.refund_amt, 3, 'ROUND_HALF_EVEN') AS refund_amt,
    ROUND(smry_pc_metric.credit_balance_amt, 3, 'ROUND_HALF_EVEN') AS credit_balance_amt,
    ROUND(smry_pc_metric.ending_balance_amt, 3, 'ROUND_HALF_EVEN') AS ending_balance_amt,
    ROUND(smry_pc_metric.wo_denial_amt, 3, 'ROUND_HALF_EVEN') AS wo_denial_amt,
    smry_pc_metric.dw_last_update_date_time,
    smry_pc_metric.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.smry_pc_metric
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_pc_metric_other.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.smry_pc_metric_other AS SELECT
    smry_pc_metric_other.service_type_name,
    smry_pc_metric_other.fact_lvl_code,
    smry_pc_metric_other.parent_code,
    smry_pc_metric_other.child_code,
    smry_pc_metric_other.ytd_month_ind,
    smry_pc_metric_other.month_id,
    ROUND(smry_pc_metric_other.overturned_denial_amt, 3, 'ROUND_HALF_EVEN') AS overturned_denial_amt,
    ROUND(smry_pc_metric_other.unpay_recovery_amt, 3, 'ROUND_HALF_EVEN') AS unpay_recovery_amt,
    ROUND(smry_pc_metric_other.over_under_amt, 3, 'ROUND_HALF_EVEN') AS over_under_amt,
    ROUND(smry_pc_metric_other.exp_rbmt_amt, 3, 'ROUND_HALF_EVEN') AS exp_rbmt_amt,
    smry_pc_metric_other.dw_last_update_date_time,
    smry_pc_metric_other.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.smry_pc_metric_other
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_rcm_mc_discrepancy_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.smry_rcm_mc_discrepancy_rate AS SELECT
    smry_rcm_mc_discrepancy_rate.coid,
    smry_rcm_mc_discrepancy_rate.company_code,
    smry_rcm_mc_discrepancy_rate.month_id,
    ROUND(smry_rcm_mc_discrepancy_rate.over_under_amt, 3, 'ROUND_HALF_EVEN') AS over_under_amt,
    ROUND(smry_rcm_mc_discrepancy_rate.exp_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS exp_reimbursement_amt,
    ROUND(smry_rcm_mc_discrepancy_rate.discrepancy_rate, 3, 'ROUND_HALF_EVEN') AS discrepancy_rate,
    smry_rcm_mc_discrepancy_rate.dw_last_update_date_time,
    smry_rcm_mc_discrepancy_rate.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.smry_rcm_mc_discrepancy_rate
;
-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_rcm_unbilled_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.smry_rcm_unbilled_inventory AS SELECT
    smry_rcm_unbilled_inventory.coid,
    smry_rcm_unbilled_inventory.company_code,
    smry_rcm_unbilled_inventory.payor_sid,
    smry_rcm_unbilled_inventory.patient_type_sid,
    smry_rcm_unbilled_inventory.month_id,
    smry_rcm_unbilled_inventory.scenario_sid,
    ROUND(smry_rcm_unbilled_inventory.in_hse_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS in_hse_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.lt_summ_days_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS lt_summ_days_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.gt_summ_days_med_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS gt_summ_days_med_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.gt_summ_days_bus_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS gt_summ_days_bus_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.error_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS error_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.error_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS error_acct_bal_amt_msc,
    ROUND(smry_rcm_unbilled_inventory.hold_acct_bal_amt_pas, 3, 'ROUND_HALF_EVEN') AS hold_acct_bal_amt_pas,
    ROUND(smry_rcm_unbilled_inventory.hold_acct_bal_amt_fac, 3, 'ROUND_HALF_EVEN') AS hold_acct_bal_amt_fac,
    ROUND(smry_rcm_unbilled_inventory.hold_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS hold_acct_bal_amt_msc,
    ROUND(smry_rcm_unbilled_inventory.hold_acct_bal_amt_final_bill, 3, 'ROUND_HALF_EVEN') AS hold_acct_bal_amt_final_bill,
    ROUND(smry_rcm_unbilled_inventory.valid_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS valid_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.valid_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS valid_acct_bal_amt_msc,
    ROUND(smry_rcm_unbilled_inventory.wait_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS wait_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.wait_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS wait_acct_bal_amt_msc,
    ROUND(smry_rcm_unbilled_inventory.add_acct_bal_amt, 3, 'ROUND_HALF_EVEN') AS add_acct_bal_amt,
    ROUND(smry_rcm_unbilled_inventory.add_acct_bal_amt_msc, 3, 'ROUND_HALF_EVEN') AS add_acct_bal_amt_msc,
    smry_rcm_unbilled_inventory.dw_last_update_date_time,
    smry_rcm_unbilled_inventory.source_system_code
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.smry_rcm_unbilled_inventory
;
