-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_eor_calculation_service.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_eor_calculation_service
   OPTIONS(description='Contains calculation service data related to explanation of reimbursement.')
  AS SELECT
      a.company_code,
      a.coid,
      a.patient_dw_id,
      a.payor_dw_id,
      a.iplan_insurance_order_num,
      a.eor_log_date,
      a.acct_payor_calc_service_id,
      a.calc_id,
      a.ce_service_id,
      a.service_date,
      a.unit_num,
      a.pat_acct_num,
      a.iplan_id,
      a.service_qty,
      a.expected_payment_amt,
      a.subterm_expected_payment_amt,
      a.service_charge_amt,
      a.exclusion_charge_amt,
      a.noncovered_charge_amt,
      a.proc_code_type_desc,
      a.proc_code,
      a.modifier_1_code,
      a.modifier_2_code,
      a.revenue_code_desc,
      a.calculation_method_desc,
      a.rate_info_from_day_num,
      a.rate_info_to_day_num,
      a.rate_info_amt,
      a.rate_info_service_desc,
      a.gov_drg_payment_amt,
      a.gov_ime_capital_drg_pmt_amt,
      a.gov_operating_cost_outlier_amt,
      a.gov_operating_fed_pmt_amt,
      a.hipps_payment_amt,
      a.ipf_pps_outlier_per_diem_amt,
      a.ipf_pps_out_per_diem_addon_amt,
      a.gov_scp_payment_amt,
      a.ipf_cost_charge_ratio_amt,
      a.ipf_fixed_loss_amt,
      a.ipf_labor_share_amt,
      a.ipf_labor_share_pct,
      a.ipf_non_labor_share_amt,
      a.ipf_teaching_adj_fctr_pct,
      a.ipf_age_fctr_pct,
      a.ipf_drg_adj_fctr_pct,
      a.ipf_comorbidity_fctr_pct,
      a.ipf_pps_facility_adj_fctr_pct,
      a.ipf_pps_adj_fctr_pct,
      a.ipf_pps_los_adj_fctr_pct,
      a.gov_new_tech_add_on_amt,
      a.gov_device_offset_red_amt,
      a.gov_transfer_pct,
      a.ipf_per_diem_day_num,
      a.apc_composite_flg,
      a.dsh_unadj_opr_add_on_amt,
      a.dsh_uncmps_care_add_on_amt,
      a.mdh_add_on_amt,
      a.low_vol_add_on_amt,
      a.readm_adj_amt,
      a.val_based_adj_amt,
      a.drg_fixed_loss_threshold_amt,
      a.drg_marginal_cost_fctr_pct,
      a.apc_outlier_pct,
      a.opps_pct,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_calculation_service AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
