-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_eom
   OPTIONS(description='Concuity End Of Month Snapshot')
  AS SELECT
      a.patient_dw_id,
      a.rptg_period,
      a.company_code,
      a.coid,
      a.unit_num,
      a.pat_acct_num,
      a.admission_date,
      a.discharge_date,
      a.final_bill_date,
      a.ar_bill_thru_date,
      a.billing_status_code,
      a.financial_class_code,
      a.patient_type_code,
      a.account_status_code,
      a.total_account_balance_amt,
      a.total_billed_charges_amt,
      a.total_payment_amt,
      a.total_adjustment_amt,
      a.total_contract_allow_amt,
      a.account_id,
      a.eor_gross_reimbursement_amt,
      a.eor_log_date,
      a.rate_schedule_name,
      a.log84_ind,
      a.prior_day_gross_reimb_amt,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eom AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
