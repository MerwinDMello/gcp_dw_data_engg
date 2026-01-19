-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_denial_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_denial_eom
   OPTIONS(description='Contains denials as of the end of a given month.')
  AS SELECT
      cc_denial_eom.patient_dw_id,
      cc_denial_eom.payor_dw_id,
      cc_denial_eom.report_end_date,
      cc_denial_eom.iplan_insurance_order_num,
      cc_denial_eom.company_code,
      cc_denial_eom.coid,
      cc_denial_eom.unit_num,
      cc_denial_eom.pat_acct_num,
      cc_denial_eom.iplan_id,
      cc_denial_eom.denial_status_code,
      cc_denial_eom.patient_type_code,
      cc_denial_eom.patient_financial_class_code,
      cc_denial_eom.payor_financial_class_code,
      cc_denial_eom.appeal_origination_date,
      cc_denial_eom.appeal_level_origination_date,
      cc_denial_eom.disposition_num,
      cc_denial_eom.appeal_level_num,
      cc_denial_eom.beginning_balance_amt,
      cc_denial_eom.beginning_balance_cnt,
      cc_denial_eom.beginning_appeal_amt,
      cc_denial_eom.new_denial_account_amt,
      cc_denial_eom.new_denial_account_cnt,
      cc_denial_eom.unworked_conversion_amt,
      cc_denial_eom.unworked_new_accounts_cnt,
      cc_denial_eom.not_true_denial_amt,
      cc_denial_eom.write_off_denial_account_amt,
      cc_denial_eom.overturned_account_amt,
      cc_denial_eom.corrections_account_amt,
      cc_denial_eom.appeal_closing_date,
      cc_denial_eom.trans_next_party_amt,
      cc_denial_eom.ending_balance_amt,
      cc_denial_eom.resolved_accounts_cnt,
      cc_denial_eom.total_charge_amt,
      cc_denial_eom.attending_physician_name_id,
      cc_denial_eom.account_balance_amt,
      cc_denial_eom.discharge_date,
      cc_denial_eom.service_code,
      cc_denial_eom.medical_record_num,
      cc_denial_eom.last_update_hca_3_4_id,
      cc_denial_eom.last_update_date,
      cc_denial_eom.work_again_date,
      cc_denial_eom.appeal_deadline_date,
      cc_denial_eom.denied_charges_amt,
      cc_denial_eom.cash_adjustment_amt,
      cc_denial_eom.ca_adjustment_amt,
      cc_denial_eom.root_cause,
      cc_denial_eom.root_cause_desc,
      cc_denial_eom.denial_code_category,
      cc_denial_eom.disposition_code,
      cc_denial_eom.appeal_num,
      cc_denial_eom.sequence_number,
      cc_denial_eom.appeal_code,
      cc_denial_eom.appeal_code_desc,
      cc_denial_eom.schema_id,
      cc_denial_eom.vendor_cd,
      cc_denial_eom.source_system_code,
      cc_denial_eom.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.cc_denial_eom
  ;
