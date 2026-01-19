-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/denial_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.denial_eom
   OPTIONS(description='Denial Month End Table captures the Denial Inventory that flows from Denial Web Apps and Concuity system.')
  AS SELECT
      a.company_code,
      a.coid,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      a.iplan_insurance_order_num,
      a.pe_date,
      a.denial_status_code,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.iplan_id,
      a.patient_type_code,
      ROUND(a.patient_financial_class_code, 0, 'ROUND_HALF_EVEN') AS patient_financial_class_code,
      ROUND(a.payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_code,
      a.disposition_num,
      a.appeal_origination_date,
      a.appeal_level_origination_date,
      a.appeal_closing_date,
      a.appeal_deadline_date,
      a.appeal_level_num,
      ROUND(a.beginning_balance_amt, 3, 'ROUND_HALF_EVEN') AS beginning_balance_amt,
      a.beginning_balance_count,
      ROUND(a.beginning_appeal_amt, 3, 'ROUND_HALF_EVEN') AS beginning_appeal_amt,
      ROUND(a.new_denial_account_amt, 3, 'ROUND_HALF_EVEN') AS new_denial_account_amt,
      a.new_denial_account_count,
      ROUND(a.not_true_denial_amt, 3, 'ROUND_HALF_EVEN') AS not_true_denial_amt,
      ROUND(a.write_off_denial_account_amt, 3, 'ROUND_HALF_EVEN') AS write_off_denial_account_amt,
      ROUND(a.unworked_conversion_amt, 3, 'ROUND_HALF_EVEN') AS unworked_conversion_amt,
      a.unworked_new_accounts_count,
      ROUND(a.overturned_account_amt, 3, 'ROUND_HALF_EVEN') AS overturned_account_amt,
      ROUND(a.corrections_account_amt, 3, 'ROUND_HALF_EVEN') AS corrections_account_amt,
      ROUND(a.trans_next_party_amt, 3, 'ROUND_HALF_EVEN') AS trans_next_party_amt,
      ROUND(a.ending_balance_amt, 3, 'ROUND_HALF_EVEN') AS ending_balance_amt,
      ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(a.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
      a.resolved_accounts_count,
      a.attending_physician_name,
      a.service_code,
      a.medical_record_num,
      a.discharge_date,
      a.last_update_id,
      a.last_update_date,
      a.work_again_date,
      a.source_system_code,
      ROUND(a.denied_charges, 3, 'ROUND_HALF_EVEN') AS denied_charges,
      ROUND(a.cc_cash_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS cc_cash_adjustment_amt,
      ROUND(a.cc_contractual_allow_adj_amt, 3, 'ROUND_HALF_EVEN') AS cc_contractual_allow_adj_amt,
      a.cc_root_cause_id,
      a.cc_denial_cat_code,
      a.cc_disposition_code,
      a.pa_vendor_code,
      a.appeal_sent_date,
      ROUND(a.cc_appeal_num, 0, 'ROUND_HALF_EVEN') AS cc_appeal_num,
      a.cc_appeal_detail_seq_num
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.denial_eom AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
