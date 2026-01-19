-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/denial_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.denial_eom AS SELECT
    a.company_code,
    a.coid,
    a.patient_dw_id,
    a.payor_dw_id,
    a.iplan_insurance_order_num,
    a.pe_date,
    a.denial_status_code,
    a.unit_num,
    a.pat_acct_num,
    a.iplan_id,
    a.patient_type_code,
    a.patient_financial_class_code,
    a.payor_financial_class_code,
    a.disposition_num,
    a.appeal_origination_date,
    a.appeal_level_origination_date,
    a.appeal_closing_date,
    a.appeal_deadline_date,
    a.appeal_level_num,
    a.beginning_balance_amt,
    a.beginning_balance_count,
    a.beginning_appeal_amt,
    a.new_denial_account_amt,
    a.new_denial_account_count,
    a.not_true_denial_amt,
    a.write_off_denial_account_amt,
    a.unworked_conversion_amt,
    a.unworked_new_accounts_count,
    a.overturned_account_amt,
    a.corrections_account_amt,
    a.trans_next_party_amt,
    a.ending_balance_amt,
    a.total_charge_amt,
    a.account_balance_amt,
    a.resolved_accounts_count,
    a.attending_physician_name,
    a.service_code,
    a.medical_record_num,
    a.discharge_date,
    a.last_update_id,
    a.last_update_date,
    a.work_again_date,
    a.source_system_code,
    a.denied_charges,
    a.cc_cash_adjustment_amt,
    a.cc_contractual_allow_adj_amt,
    a.cc_root_cause_id,
    a.cc_denial_cat_code,
    a.cc_disposition_code,
    a.pa_vendor_code,
    a.appeal_sent_date,
    a.cc_appeal_num,
    a.cc_appeal_detail_seq_num
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.denial_eom AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
