-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_eor_calculation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_eor_calculation
   OPTIONS(description='Contains calcultion data related to explanation of reimbursement.')
  AS SELECT
      a.company_code,
      a.coid,
      a.patient_dw_id,
      a.payor_dw_id,
      a.iplan_insurance_order_num,
      a.eor_log_date,
      a.log_id,
      a.log_sequence_num,
      a.eff_from_date,
      a.unit_num,
      a.pat_acct_num,
      a.iplan_id,
      a.interest_calc_date,
      a.interest_rate,
      a.interest_days_num,
      a.interest_amt,
      a.interest_stop_date,
      a.first_denial_date,
      a.length_of_stay_days_num,
      a.length_of_service_days_num,
      a.billing_status_code,
      a.calc_lock_ind,
      a.calc_success_ind,
      a.allow_contract_code_change_ind,
      a.payer_eligible_ind,
      a.owner_override_ind,
      a.reason_override_ind,
      a.status_override_ind,
      a.active_ind,
      a.calc_base_id,
      a.cob_method_id,
      a.cers_term_id,
      a.account_payer_status_id,
      a.calc_id,
      a.appeal_id,
      a.icd_version_desc,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_calculation AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
