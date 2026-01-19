-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_pars_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.fact_rcom_pars_denial
   OPTIONS(description='This is Monthly Denial Inventory for HCA and Parallon Customers.')
  AS SELECT
      ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      a.date_sid,
      a.payor_sid,
      a.payor_financial_class_sid,
      a.payor_sequence_sid,
      a.patient_type_sid,
      a.appeal_disp_sid,
      a.appeal_code_sid,
      a.discharge_age_month_sid,
      a.denial_orig_age_month_sid,
      a.appeal_age_month_sid,
      a.service_code_sid,
      a.unit_num_sid,
      a.los_sid,
      a.drg_sid,
      a.admission_type_sid,
      a.denial_type_sid,
      a.appeal_root_cause_sid,
      a.scenario_sid,
      a.source_sid,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.iplan_id,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      a.coid,
      a.company_code,
      a.same_store_sid,
      a.resolved_denial_age_month_id,
      a.resolved_discharge_age_month_id,
      a.dollar_strf_sid,
      a.person_role_code,
      CASE
        WHEN session_user() = pn.userid THEN '***'
        ELSE a.patient_full_name
      END AS patient_full_name,
      a.patient_birth_date,
      ROUND(a.beginning_balance_amt, 3, 'ROUND_HALF_EVEN') AS beginning_balance_amt,
      a.beginning_balance_count,
      ROUND(a.beginning_appeal_amt, 3, 'ROUND_HALF_EVEN') AS beginning_appeal_amt,
      ROUND(a.ending_balance_amt, 3, 'ROUND_HALF_EVEN') AS ending_balance_amt,
      a.ending_balance_count,
      ROUND(a.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
      ROUND(a.write_off_denial_account_amt, 3, 'ROUND_HALF_EVEN') AS write_off_denial_account_amt,
      a.write_off_denial_account_count,
      ROUND(a.new_denial_account_amt, 3, 'ROUND_HALF_EVEN') AS new_denial_account_amt,
      a.new_denial_account_count,
      ROUND(a.not_true_denial_amt, 3, 'ROUND_HALF_EVEN') AS not_true_denial_amt,
      a.not_true_denial_count,
      ROUND(a.corrections_account_amt, 3, 'ROUND_HALF_EVEN') AS corrections_account_amt,
      a.corrections_account_count,
      ROUND(a.overturned_account_amt, 3, 'ROUND_HALF_EVEN') AS overturned_account_amt,
      a.overturned_account_count,
      ROUND(a.cc_corrections_account_amt, 3, 'ROUND_HALF_EVEN') AS cc_corrections_account_amt,
      a.cc_corrections_account_cnt,
      ROUND(a.cc_overturned_account_amt, 3, 'ROUND_HALF_EVEN') AS cc_overturned_account_amt,
      a.cc_overturned_account_cnt,
      ROUND(a.trans_next_party_amt, 3, 'ROUND_HALF_EVEN') AS trans_next_party_amt,
      a.trans_next_party_count,
      ROUND(a.unworked_conversion_amt, 3, 'ROUND_HALF_EVEN') AS unworked_conversion_amt,
      a.unworked_conversion_count,
      a.resolved_accounts_count,
      ROUND(a.below_threshold_amt, 3, 'ROUND_HALF_EVEN') AS below_threshold_amt,
      a.below_threshold_count,
      a.appeal_closing_date,
      a.appeal_level_origination_date,
      a.appeal_deadline_date,
      a.appeal_level_num,
      a.work_again_date,
      CASE
        WHEN a.patient_sid = -1
         AND a.source_sid = 8
         AND a.patient_dw_id = -1
         AND a.attending_physician_name IS NOT NULL THEN a.attending_physician_name
        ELSE CAST(NULL as STRING)
      END AS lumpsum_payor_gen03,
      CASE
        WHEN a.attending_physician_name IS NOT NULL
         AND (a.patient_sid = -1
         AND a.source_sid = 8
         AND a.patient_dw_id = -1) THEN CAST(NULL as STRING)
        ELSE a.attending_physician_name
      END AS attending_physician_name,
      ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      a.last_update_id,
      a.last_update_date,
      a.appeal_origination_date,
      a.discharge_date,
      a.resolved_days,
      a.open_days,
      a.overturned_days,
      a.final_bill_count,
      ROUND(a.final_bill_charge_amt, 3, 'ROUND_HALF_EVEN') AS final_bill_charge_amt,
      ROUND(a.denied_charge_amt, 3, 'ROUND_HALF_EVEN') AS denied_charge_amt,
      ROUND(a.cash_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS cash_adjustment_amt,
      ROUND(a.contractual_allow_adj_amt, 3, 'ROUND_HALF_EVEN') AS contractual_allow_adj_amt,
      ROUND(a.cc_appeal_num, 0, 'ROUND_HALF_EVEN') AS cc_appeal_num,
      a.cc_appeal_detail_seq_num,
      ROUND(a.cc_appeal_crnt_bal_amt, 3, 'ROUND_HALF_EVEN') AS cc_appeal_crnt_bal_amt,
      a.pa_vendor_code,
      a.prepay_mc_flag,
      a.pe_date,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.fact_rcom_pars_denial AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            {{ params.param_sec_base_views_dataset_name }}.security_mask_and_exception
          WHERE security_mask_and_exception.userid = session_user()
           AND upper(security_mask_and_exception.masked_column_code) = 'PN'
      ) AS pn ON pn.userid = session_user()
  ;
