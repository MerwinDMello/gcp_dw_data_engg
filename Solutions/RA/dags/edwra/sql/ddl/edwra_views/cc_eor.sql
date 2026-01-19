-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_eor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_eor AS SELECT
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
    a.financial_class_code,
    a.final_bill_date,
    a.ar_bill_thru_date,
    a.ar_bill_drop_date,
    a.contract_begin_date,
    a.eor_transaction_date,
    a.bill_reason_code,
    a.eor_drg_code,
    a.cc_drg_code,
    a.cc_drg_weight,
    a.eor_hipps_code,
    a.cc_cmg_code,
    a.cc_cmg_weight,
    a.eor_covered_days_num,
    a.eor_total_charge_amt,
    a.expected_insurance_payment_amt,
    a.pat_resp_non_covered_chg_amt,
    a.total_actual_pat_resp_amt,
    a.eor_non_covered_charge_amt,
    a.eor_cmptd_reimbursement_amt,
    a.eor_deductible_amt,
    a.eor_coinsurance_amt,
    a.eor_copay_amt,
    a.eor_insurance_payment_amt,
    a.eor_contractual_allowance_amt,
    a.eor_cost_of_service_amt,
    a.month_13_ind,
    a.outlier_code,
    a.auto_post_amt,
    a.auto_post_ind,
    a.drg_table_id,
    a.opps_ind,
    a.inpatient_outpatient_ind,
    a.visit_count,
    a.eor_gross_reimbursement_amt,
    a.eor_net_reimbursement_amt,
    a.eor_non_bill_charge_amt,
    a.sqstrtn_red_amt,
    a.model_name,
    a.project_desc,
    a.eor_ipf_interrupted_day_stay,
    a.calc_success_ind,
    a.latest_calc_ind,
    a.interim_bill_ind,
    a.financial_period_id,
    a.reason_id,
    a.calc_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor AS a
  WHERE (a.company_code, a.coid) IN(
    SELECT AS STRUCT
        secref_facility.company_code,
        secref_facility.co_id
      FROM
        {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility
      WHERE rtrim(secref_facility.user_id) = rtrim(session_user())
  )
;
