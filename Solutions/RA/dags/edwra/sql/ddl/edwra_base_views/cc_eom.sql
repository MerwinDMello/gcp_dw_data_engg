-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eom
   OPTIONS(description='Concuity End Of Month Snapshot')
  AS SELECT
      cc_eom.patient_dw_id,
      cc_eom.rptg_period,
      cc_eom.company_code,
      cc_eom.coid,
      cc_eom.unit_num,
      cc_eom.pat_acct_num,
      cc_eom.admission_date,
      cc_eom.discharge_date,
      cc_eom.final_bill_date,
      cc_eom.ar_bill_thru_date,
      cc_eom.billing_status_code,
      cc_eom.financial_class_code,
      cc_eom.patient_type_code,
      cc_eom.account_status_code,
      cc_eom.total_account_balance_amt,
      cc_eom.total_billed_charges_amt,
      cc_eom.total_payment_amt,
      cc_eom.total_adjustment_amt,
      cc_eom.total_contract_allow_amt,
      cc_eom.account_id,
      cc_eom.eor_gross_reimbursement_amt,
      cc_eom.eor_log_date,
      cc_eom.rate_schedule_name,
      cc_eom.log84_ind,
      cc_eom.prior_day_gross_reimb_amt,
      cc_eom.source_system_code,
      cc_eom.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.cc_eom
  ;
