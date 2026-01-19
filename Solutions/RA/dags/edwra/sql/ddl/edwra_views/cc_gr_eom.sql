-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_gr_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_gr_eom
   OPTIONS(description='This table will contain government reimbursment departments reporting month end (2nd through 5th) data. BOBJ produced excel files are the source data for this table.')
  AS SELECT
      a.patient_dw_id,
      a.report_name,
      a.period_end_date,
      a.report_run_date_time,
      a.section_name,
      a.reporting_period,
      a.company_code,
      a.coid,
      a.facility_name,
      a.unit_num,
      a.pat_acct_num,
      CASE
        WHEN rtrim(session_user()) = rtrim(pn.userid) THEN '***'
        ELSE a.patient_name
      END AS patient_name,
      a.rate_schedule_name,
      a.section_month,
      a.cost_report_year,
      a.account_status_code,
      a.auto_posted_flag,
      a.cmg_code,
      a.drg_code,
      a.ra_drg_code,
      a.cmg_weight_amt,
      a.drg_weight_amt,
      a.prorated_drg_weight_amt,
      a.exp_hipps_cmg_code,
      a.ra_hipps_cmg_code,
      a.var_drg_flag,
      a.var_cmg_flag,
      a.discharge_admit_date,
      a.claim_submit_date,
      a.ra_payment_date,
      a.covered_days_los_cnt,
      a.days_billed_cnt,
      a.discharge_days_cnt,
      a.ra_covered_days_cnt,
      a.remit_cnt,
      a.var_covered_days_cnt,
      a.account_balance_amt,
      a.accrued_capital_subtotal_amt,
      a.accrued_operating_subtotal_amt,
      a.accrued_payment_amt,
      a.apc_charges_amt,
      a.apc_payment_amt,
      a.drg_arithmetic_los_amt,
      a.capital_payment_subtotal_amt,
      a.cmac_charge_amt,
      a.cmac_payment_amt,
      a.cmg_payment_amt,
      a.coinsurance_deductible_amt,
      a.computed_reimb_amt,
      a.cost_outlier_payment_amt,
      a.covered_charges_amt,
      a.drg_payment_amt,
      a.ect_payment_amt,
      a.education_payment_amt,
      a.fee_based_payment_amt,
      a.idme_payment_amt,
      a.lab_charge_amt,
      a.lab_payment_amt,
      a.lip_payment_amt,
      a.non_covered_charges_amt,
      a.operating_drg_payment_amt,
      a.operating_dsh_amt,
      a.operating_outlier_amt,
      a.operating_cost_outlier_amt,
      a.other_service_charge_amt,
      a.other_service_payment_amt,
      a.outlier_payment_amt,
      a.payment_adj_amt,
      a.ra_coinsurance_amt,
      a.ra_contractual_amt,
      a.ra_covered_charge_amt,
      a.ra_deductible_amt,
      a.ra_drg_amt,
      a.ra_idme_amt,
      a.ra_lab_payment_amt,
      a.ra_operating_dsh_amt,
      a.ra_operating_idme_amt,
      a.ra_operating_outlier_amt,
      a.ra_outlier_amt,
      a.ra_therapy_payment_amt,
      a.ra_total_capital_amt,
      a.ra_total_payment_amt,
      a.replace_device_reduction_amt,
      a.rug_payment_amt,
      a.sequestration_amt,
      a.short_stay_outlier_payment_amt,
      a.tech_add_on_amt,
      a.tech_add_on_payment_amt,
      a.therapy_charge_amt,
      a.therapy_payment_amt,
      a.total_charge_amt,
      a.total_expected_payment_amt,
      a.total_opr_cptl_payment_amt,
      a.total_payment_amt,
      a.transfer_payment_amt,
      a.var_coinsurance_amt,
      a.var_contractual_amt,
      a.var_covered_charges_amt,
      a.var_deductible_amt,
      a.var_imed_educ_amt,
      a.var_lab_payment_amt,
      a.var_operating_dsh_amt,
      a.var_operating_ime_amt,
      a.var_operating_outlier_amt,
      a.var_outlier_amt,
      a.var_therapy_payment_amt,
      a.var_total_capital_amt,
      a.var_total_payment_amt,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_gr_eom AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            `{{ params.param_parallon_cur_project_id }}`.edw_sec_base_views.security_mask_and_exception
          WHERE rtrim(security_mask_and_exception.userid) = rtrim(session_user())
           AND rtrim(security_mask_and_exception.masked_column_code) = 'PN'
      ) AS pn ON rtrim(pn.userid) = rtrim(session_user())
  ;
