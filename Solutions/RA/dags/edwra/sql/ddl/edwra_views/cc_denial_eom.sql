-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_denial_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_denial_eom
   OPTIONS(description='Contains denials as of the end of a given month.')
  AS SELECT
      a.patient_dw_id,
      a.payor_dw_id,
      a.report_end_date,
      a.iplan_insurance_order_num,
      a.company_code,
      a.coid,
      a.unit_num,
      a.pat_acct_num,
      a.iplan_id,
      a.denial_status_code,
      a.patient_type_code,
      a.patient_financial_class_code,
      a.payor_financial_class_code,
      a.appeal_origination_date,
      a.appeal_level_origination_date,
      a.disposition_num,
      a.appeal_level_num,
      a.beginning_balance_amt,
      a.beginning_balance_cnt,
      a.beginning_appeal_amt,
      a.new_denial_account_amt,
      a.new_denial_account_cnt,
      a.unworked_conversion_amt,
      a.unworked_new_accounts_cnt,
      a.not_true_denial_amt,
      a.write_off_denial_account_amt,
      a.overturned_account_amt,
      a.corrections_account_amt,
      a.appeal_closing_date,
      a.trans_next_party_amt,
      a.ending_balance_amt,
      a.resolved_accounts_cnt,
      a.total_charge_amt,
      a.attending_physician_name_id,
      a.account_balance_amt,
      a.discharge_date,
      a.service_code,
      a.medical_record_num,
      a.last_update_hca_3_4_id,
      a.last_update_date,
      a.work_again_date,
      a.appeal_deadline_date,
      a.denied_charges_amt,
      a.cash_adjustment_amt,
      a.ca_adjustment_amt,
      a.root_cause,
      a.root_cause_desc,
      a.denial_code_category,
      a.disposition_code,
      a.appeal_num,
      a.sequence_number,
      a.appeal_code,
      a.appeal_code_desc,
      a.schema_id,
      a.vendor_cd,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_denial_eom AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
