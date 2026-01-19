-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_pars_denial.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_rcom_pars_denial AS SELECT
    a.patient_sid,
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
    a.patient_dw_id,
    a.iplan_id,
    a.payor_dw_id,
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
    a.beginning_balance_amt,
    a.beginning_balance_count,
    a.beginning_appeal_amt,
    a.ending_balance_amt,
    a.ending_balance_count,
    a.account_balance_amt,
    a.write_off_denial_account_amt,
    a.write_off_denial_account_count,
    a.new_denial_account_amt,
    a.new_denial_account_count,
    a.not_true_denial_amt,
    a.not_true_denial_count,
    a.corrections_account_amt,
    a.corrections_account_count,
    a.overturned_account_amt,
    a.overturned_account_count,
    a.cc_corrections_account_amt,
    a.cc_corrections_account_cnt,
    a.cc_overturned_account_amt,
    a.cc_overturned_account_cnt,
    a.trans_next_party_amt,
    a.trans_next_party_count,
    a.unworked_conversion_amt,
    a.unworked_conversion_count,
    a.resolved_accounts_count,
    a.below_threshold_amt,
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
      ELSE NULL
    END AS lumpsum_payor_gen03,
    CASE
      WHEN a.attending_physician_name IS NOT NULL
       AND (a.patient_sid = -1
       AND a.source_sid = 8
       AND a.patient_dw_id = -1) THEN NULL
      ELSE a.attending_physician_name
    END AS attending_physician_name,
    a.total_charge_amt,
    a.last_update_id,
    a.last_update_date,
    a.appeal_origination_date,
    a.discharge_date,
    a.resolved_days,
    a.open_days,
    a.overturned_days,
    a.final_bill_count,
    a.final_bill_charge_amt,
    a.denied_charge_amt,
    a.cash_adjustment_amt,
    a.contractual_allow_adj_amt,
    a.cc_appeal_num,
    a.cc_appeal_detail_seq_num,
    a.cc_appeal_crnt_bal_amt,
    a.pa_vendor_code,
    a.prepay_mc_flag,
    a.pe_date,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_denial AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          `hca-hin-dev-cur-parallon`.edw_sec_base_views.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND security_mask_and_exception.masked_column_code = 'PN'
    ) AS pn ON pn.userid = session_user()
;
