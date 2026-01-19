-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_eor_calculation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_calculation
   OPTIONS(description='Contains calcultion data related to explanation of reimbursement.')
  AS SELECT
      cc_eor_calculation.company_code,
      cc_eor_calculation.coid,
      cc_eor_calculation.patient_dw_id,
      cc_eor_calculation.payor_dw_id,
      cc_eor_calculation.iplan_insurance_order_num,
      cc_eor_calculation.eor_log_date,
      cc_eor_calculation.log_id,
      cc_eor_calculation.log_sequence_num,
      cc_eor_calculation.eff_from_date,
      cc_eor_calculation.unit_num,
      cc_eor_calculation.pat_acct_num,
      cc_eor_calculation.iplan_id,
      cc_eor_calculation.interest_calc_date,
      cc_eor_calculation.interest_rate,
      cc_eor_calculation.interest_days_num,
      cc_eor_calculation.interest_amt,
      cc_eor_calculation.interest_stop_date,
      cc_eor_calculation.first_denial_date,
      cc_eor_calculation.length_of_stay_days_num,
      cc_eor_calculation.length_of_service_days_num,
      cc_eor_calculation.billing_status_code,
      cc_eor_calculation.calc_lock_ind,
      cc_eor_calculation.calc_success_ind,
      cc_eor_calculation.allow_contract_code_change_ind,
      cc_eor_calculation.payer_eligible_ind,
      cc_eor_calculation.owner_override_ind,
      cc_eor_calculation.reason_override_ind,
      cc_eor_calculation.status_override_ind,
      cc_eor_calculation.active_ind,
      cc_eor_calculation.calc_base_id,
      cc_eor_calculation.cob_method_id,
      cc_eor_calculation.cers_term_id,
      cc_eor_calculation.account_payer_status_id,
      cc_eor_calculation.calc_id,
      cc_eor_calculation.appeal_id,
      cc_eor_calculation.icd_version_desc,
      cc_eor_calculation.dw_last_update_date_time,
      cc_eor_calculation.source_system_code
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_calculation
  ;
