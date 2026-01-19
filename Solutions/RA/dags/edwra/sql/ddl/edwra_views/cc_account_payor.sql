-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_account_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_account_payor
   OPTIONS(description='Contains all account payer records related to work queue.')
  AS SELECT
      a.patient_dw_id,
      a.payor_dw_id,
      a.iplan_order_num,
      a.company_code,
      a.coid,
      a.unit_num,
      a.pat_acct_num,
      a.iplan_id,
      a.employer_name,
      a.insurance_group_name,
      a.insurance_group_num_cd,
      CASE
        WHEN rtrim(session_user()) = rtrim(pn.userid) THEN '***'
        ELSE a.insured_name
      END AS insured_name,
      a.insured_gender_cd,
      a.insured_birth_date,
      a.payor_identification_num_cd,
      a.authorization_cd,
      CASE
        WHEN rtrim(session_user()) = rtrim(pn.userid) THEN '***'
        ELSE a.billing_name
      END AS billing_name,
      a.billed_date,
      a.billing_address_1,
      a.billing_address_2,
      a.billing_city_name,
      a.billing_state_code,
      a.billing_zip_code,
      a.billing_phone_num_cd,
      a.billing_fax_num,
      a.billing_contact_person_name,
      a.billing_contact_email_name,
      a.relation_to_insured_pat_cd,
      a.drg_version,
      a.pa_denial_date,
      a.first_denial_date,
      a.interest_stop_date,
      a.work_queue_name,
      a.rate_schedule_name,
      a.calc_date,
      a.calc_num,
      a.calc_result_ind,
      a.calc_base_choice_resolved_ind,
      a.calc_lock_ind,
      a.pa_autm_post_ind,
      a.apc_group_ind,
      a.eligible_ind,
      a.account_calc_situation_ind,
      a.allow_contract_code_change_ind,
      a.change_claim_trigger_ind,
      a.manual_trigger_ind,
      a.life_check_eligible_ind,
      a.pa_financial_class_code,
      a.bill_reason_code,
      a.external_appeal_code,
      a.total_exp_payment_amt,
      a.total_payment_amt,
      a.total_denial_amt,
      a.total_pat_responsibility_amt,
      a.est_pat_responsibility_amt,
      a.actual_pat_responsibility_amt,
      a.total_exp_contractual_amt,
      a.total_adjustment_amt,
      a.total_variance_adjustment_amt,
      a.current_exp_payment_amt,
      a.current_exp_contractual_amt,
      a.prof_covered_charge_amt,
      a.prof_exp_payment_amt,
      a.pa_prof_part_b_charge_amt,
      a.pa_blood_deductible_amt,
      a.sqstrtn_reduction_amt,
      a.pa_coinsurance_amt,
      a.autm_post_amt,
      a.coinsurance_amt,
      a.copay_amt,
      a.deductible_amt,
      a.covered_charge_instu_amt,
      a.cmpt_method_choice_id,
      a.calc_base_id,
      a.calc_base_survivor_id,
      a.reason_id,
      a.account_payor_status_id,
      a.appeal_id,
      a.cers_profile_id,
      a.capping_method_id,
      a.cers_term_id,
      a.project_id,
      a.account_payor_id,
      a.creation_date,
      a.update_date,
      a.prepay_mgd_care_sw,
      a.ip_provider_num,
      a.op_provider_num,
      a.payer_type_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_payor AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            `{{ params.param_parallon_cur_project_id }}`.edw_sec_base_views.security_mask_and_exception
          WHERE rtrim(security_mask_and_exception.userid) = rtrim(session_user())
           AND rtrim(security_mask_and_exception.masked_column_code) = 'PN'
      ) AS pn ON rtrim(pn.userid) = rtrim(session_user())
  ;
