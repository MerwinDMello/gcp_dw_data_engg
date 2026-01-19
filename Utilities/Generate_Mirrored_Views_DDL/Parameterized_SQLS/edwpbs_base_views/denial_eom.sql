-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/denial_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.denial_eom
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
      {{ params.param_pbs_core_dataset_name }}.denial_eom
  ;
