-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/bi_cc_denial_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.bi_cc_denial_eom AS SELECT
    ff.company_code,
    ff.coid,
    deom.report_end_date,
    deom.denial_status_code,
    deom.unit_num,
    deom.pat_acct_num,
    deom.iplan_id,
    deom.iplan_insurance_order_num,
    deom.patient_type_code,
    deom.patient_financial_class_code,
    deom.payor_financial_class_code,
    deom.appeal_origination_date,
    deom.appeal_level_origination_date,
    deom.disposition_num,
    deom.appeal_level_num,
    deom.beginning_balance_amt,
    deom.beginning_balance_cnt,
    deom.beginning_appeal_amt,
    deom.new_denial_account_amt,
    deom.new_denial_account_cnt,
    deom.unworked_conversion_amt,
    deom.unworked_new_accounts_cnt,
    deom.not_true_denial_amt,
    deom.write_off_denial_account_amt,
    deom.overturned_account_amt,
    deom.corrections_account_amt,
    deom.appeal_closing_date,
    deom.trans_next_party_amt,
    deom.ending_balance_amt,
    deom.resolved_accounts_cnt,
    deom.total_charge_amt,
    deom.attending_physician_name_id,
    deom.account_balance_amt,
    deom.discharge_date,
    deom.service_code,
    deom.medical_record_num,
    deom.last_update_hca_3_4_id,
    deom.last_update_date,
    deom.work_again_date,
    deom.appeal_deadline_date,
    deom.denied_charges_amt,
    deom.cash_adjustment_amt,
    deom.ca_adjustment_amt,
    deom.root_cause,
    deom.root_cause_desc,
    deom.denial_code_category,
    deom.disposition_code,
    deom.sequence_number,
    deom.appeal_code,
    deom.appeal_code_desc,
    deom.dw_last_update_date_time,
    deom.source_system_code
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.cc_denial_eom AS deom
    INNER JOIN {{ params.param_parallon_ra_views_dataset_name }}.fact_facility AS ff ON rtrim(ff.unit_num) = rtrim(deom.unit_num)
;
