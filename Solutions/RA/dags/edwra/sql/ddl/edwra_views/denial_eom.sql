-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/denial_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.denial_eom AS SELECT
    bv.company_code,
    bv.coid,
    bv.patient_dw_id,
    bv.payor_dw_id,
    bv.iplan_insurance_order_num,
    bv.pe_date,
    bv.denial_status_code,
    bv.unit_num,
    bv.pat_acct_num,
    bv.iplan_id,
    bv.patient_type_code,
    bv.patient_financial_class_code,
    bv.payor_financial_class_code,
    bv.disposition_num,
    bv.appeal_origination_date,
    bv.appeal_level_origination_date,
    bv.appeal_closing_date,
    bv.appeal_deadline_date,
    bv.appeal_level_num,
    bv.beginning_balance_amt,
    bv.beginning_balance_count,
    bv.beginning_appeal_amt,
    bv.new_denial_account_amt,
    bv.new_denial_account_count,
    bv.not_true_denial_amt,
    bv.write_off_denial_account_amt,
    bv.unworked_conversion_amt,
    bv.unworked_new_accounts_count,
    bv.overturned_account_amt,
    bv.corrections_account_amt,
    bv.trans_next_party_amt,
    bv.ending_balance_amt,
    bv.total_charge_amt,
    bv.account_balance_amt,
    bv.resolved_accounts_count,
    bv.attending_physician_name,
    bv.service_code,
    bv.medical_record_num,
    bv.discharge_date,
    bv.last_update_id,
    bv.last_update_date,
    bv.work_again_date,
    bv.source_system_code,
    bv.denied_charges,
    bv.cc_cash_adjustment_amt,
    bv.cc_contractual_allow_adj_amt,
    bv.cc_root_cause_id,
    bv.cc_denial_cat_code,
    bv.cc_disposition_code,
    bv.pa_vendor_code
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.denial_eom AS bv
    INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS sf ON rtrim(bv.coid) = rtrim(sf.co_id)
     AND rtrim(sf.user_id) = rtrim(session_user())
;
