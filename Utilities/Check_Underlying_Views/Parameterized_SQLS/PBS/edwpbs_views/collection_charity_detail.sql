-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/collection_charity_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.collection_charity_detail
   OPTIONS(description='A daily pull from Artiva system, that has the Charity data for financial review with other EDW data.')
  AS SELECT
      a.reporting_date,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.artiva_instance_code,
      a.approve_deny_date,
      a.charity_review_aging_date,
      a.appl_approval_level_num,
      a.charity_review_start_date,
      a.coid,
      a.company_code,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.iplan_id,
      a.appeal_ind,
      a.form_1099_returned_ind,
      a.charity_process_age_num,
      a.charity_approve_deny_ind,
      a.charity_approve_deny_user_id,
      a.approval_level_1_user_id,
      a.approval_level_1_date,
      a.approval_level_2_user_id,
      a.approval_level_2_date,
      a.approval_level_3_user_id,
      a.approval_level_3_date,
      a.approval_level_4_user_id,
      a.approval_level_4_date,
      a.approval_level_5_user_id,
      a.approval_level_5_date,
      a.bank_statement_returned_ind,
      a.appl_completed_user_id,
      a.credit_report_returned_ind,
      a.approval_level_num,
      ROUND(a.charity_denial_amt, 3, 'ROUND_HALF_EVEN') AS charity_denial_amt,
      a.denial_reason_code,
      ROUND(a.discount_amt, 3, 'ROUND_HALF_EVEN') AS discount_amt,
      ROUND(a.discount_pct, 3, 'ROUND_HALF_EVEN') AS discount_pct,
      a.poverty_level_by_dos_ind,
      a.extenuating_circumstance_code,
      a.extenuating_circumstance_other_desc,
      a.charity_review_end_date,
      a.faa_returned_ind,
      a.faa_flag_date,
      a.faa_family_size_num,
      a.fed_1040_returned_ind,
      ROUND(a.household_income_amt, 3, 'ROUND_HALF_EVEN') AS household_income_amt,
      a.unemployed_last_work_date,
      a.fed_poverty_guideline_met_ind,
      a.other_document_returned_ind,
      a.other_document_desc,
      a.charity_poverty_level_num,
      a.qmb_returned_ind,
      a.sa_approve_deny_flag,
      a.sa_family_size_num,
      ROUND(a.sa_income_amt, 3, 'ROUND_HALF_EVEN') AS sa_income_amt,
      a.sa_povery_level_num,
      a.sa_response_date,
      a.sa_sent_date,
      a.send_to_sa_value_ind,
      a.state_tax_returned_ind,
      a.w2_returned_ind,
      a.employer_wage_return_ind,
      ROUND(a.resp_party_weekly_income_amt, 3, 'ROUND_HALF_EVEN') AS resp_party_weekly_income_amt,
      ROUND(a.resp_party_monthly_income_amt, 3, 'ROUND_HALF_EVEN') AS resp_party_monthly_income_amt,
      ROUND(a.resp_party_yearly_income_amt, 3, 'ROUND_HALF_EVEN') AS resp_party_yearly_income_amt,
      ROUND(a.spouse_weekly_income_amt, 3, 'ROUND_HALF_EVEN') AS spouse_weekly_income_amt,
      ROUND(a.spouse_monthly_income_amt, 3, 'ROUND_HALF_EVEN') AS spouse_monthly_income_amt,
      ROUND(a.spouse_yearly_income_amt, 3, 'ROUND_HALF_EVEN') AS spouse_yearly_income_amt,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.collection_charity_detail AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
