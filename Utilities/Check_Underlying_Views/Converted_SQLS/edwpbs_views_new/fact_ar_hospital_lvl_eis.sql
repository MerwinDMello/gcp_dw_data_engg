-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_ar_hospital_lvl_eis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

DECLARE cw_const STRING DEFAULT bqutil.fn.cw_td_normalize_number(substr(format_date('%Y%m', CASE
  WHEN extract(DAY from current_date('US/Central')) < 6 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
  ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
END), 1, 6));
CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_ar_hospital_lvl_eis AS SELECT
    CASE
      WHEN a.company_code = 'H'
       AND same_store.same_store_ind = 'Y' THEN 1
      ELSE 0
    END AS same_store_sid,
    a.company_code,
    a.date_sid,
    a.scenario_sid,
    a.unit_num_sid,
    a.week_of_month_sid,
    a.source_sid,
    a.cash_receipt_amt AS cash_receipts,
    a.recoveries_wo_amt_cons65200 AS cons65200,
    a.credit_coll_svc_amt_cons83320 AS cons83320,
    a.allow_govt_receivable_fs05300 AS fs05300,
    a.allow_uncoll_amt_fs05350 AS fs05350,
    a.allow_uncoll_nonpat_amtfs05360 AS fs05360,
    a.ip_revenue_routine_amt_fs50100 AS fs50100,
    a.ip_rev_ancillary_amt_fs50200 AS fs50200,
    a.op_ancillary_rev_amt_fs50400 AS fs50400,
    a.other_operating_income_fs50900 AS fs50900,
    a.mcare_cy_cont_ip_amt_fs60100 AS fs60100,
    a.mcare_cy_cont_op_amt_fs60125 AS fs60125,
    a.prior_yr_cont_ip_amt_fs60150 AS fs60150,
    a.prior_yr_cont_op_amt_fs60175 AS fs60175,
    a.mcaid_cy_cont_ip_amt_fs60200 AS fs60200,
    a.mcaid_cy_cont_op_amt_fs60225 AS fs60225,
    a.champ_cy_cont_ip_amt_fs60300 AS fs60300,
    a.champ_cy_cont_op_amt_fs60325 AS fs60325,
    a.bc_hmo_ppo_disc_ip_amt_fs60400 AS fs60400,
    a.bc_hmo_ppo_disc_op_amt_fs60425 AS fs60425,
    a.mcare_mgd_care_ip_amt_fs60450 AS fs60450,
    a.mcare_mgd_care_op_amt_fs60460 AS fs60460,
    a.mcaid_mgd_care_ip_amt_fs60475 AS fs60475,
    a.mcaid_mgd_care_op_amt_fs60480 AS fs60480,
    a.charity_ip_amt_fs60500 AS fs60500,
    a.charity_op_amt_fs60525 AS fs60525,
    a.other_deduction_ip_amt_fs60600 AS fs60600,
    a.other_deduction_op_amt_fs60625 AS fs60625,
    a.salaries_fdept_620_amt_fs65050 AS fs65050_dept620,
    a.emp_ben_fdept_620_amt_fs65100 AS fs65100_dept620,
    a.bad_debt_amt_fs65500 AS fs65500,
    a.patient_receivable_amt_fs90000 AS fs90000,
    a.reversal_for_05350_amt_fs90008 AS fs90008,
    a.total_patient_rev_amt_fs90230 AS fs90230,
    a.curr_year_cont_amt_fs90245 AS fs90245,
    a.policy_adjustments_amt_fs90246 AS fs90246,
    a.tot_ar_cost_fdept620_gla501000 AS gla501000_dept620,
    a.mcare_clrng_gla110101_110179 AS gla110101_gla110179,
    a.mcare_clrng_gla110189_110197 AS gla110189_gla110197,
    a.mcaid_clrng_gla110201_110279 AS gla110201_gla110279,
    a.mcaid_clrng_gla110286_110297 AS gla110286_gla110297,
    a.credit_refund_amt AS refunds,
    a.reg_accuracy_reg_entry_cnt AS reg_accuracy_entries,
    a.reg_accuracy_reg_changes_cnt AS reg_accuracy_changes,
    a.ins_gt_150_day_pct_amt AS ins_gt_150,
    a.ins_gt_90_day_pct_amt AS ins_gt_90,
    a.write_off_amt,
    CAST(0 as NUMERIC) AS unbilled_bus_ofc,
    CAST(0 as NUMERIC) AS unbilled_med_rec,
    a.health_exchange_ip_amt_fs60490 AS fs60490,
    a.health_exchange_op_amt_fs60495 AS fs60495
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.fact_rcom_ar_hospital_level AS a
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS same_store ON same_store.coid = a.coid
     AND same_store.gl_close_ind = 'N'
     AND same_store.month_id = CAST(cw_const as FLOAT64)
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON b.co_id = a.coid
     AND b.company_code = a.company_code
     AND b.user_id = session_user()
UNION ALL
SELECT
    CASE
      WHEN a.company_code = 'H'
       AND same_store.same_store_ind = 'Y' THEN 1
      ELSE 0
    END AS same_store_sid,
    a.company_code,
    a.date_sid,
    a.scenario_sid,
    a.unit_num_sid,
    a.week_of_month_sid,
    a.source_sid,
    a.cash_receipt_amt AS cash_receipts,
    a.recoveries_wo_amt_cons65200 AS cons65200,
    a.credit_coll_svc_amt_cons83320 AS cons83320,
    a.allow_govt_receivable_fs05300 AS fs05300,
    a.allow_uncoll_amt_fs05350 AS fs05350,
    a.allow_uncoll_nonpat_amtfs05360 AS fs05360,
    a.ip_revenue_routine_amt_fs50100 AS fs50100,
    a.ip_rev_ancillary_amt_fs50200 AS fs50200,
    a.op_ancillary_rev_amt_fs50400 AS fs50400,
    a.other_operating_income_fs50900 AS fs50900,
    a.mcare_cy_cont_ip_amt_fs60100 AS fs60100,
    a.mcare_cy_cont_op_amt_fs60125 AS fs60125,
    a.prior_yr_cont_ip_amt_fs60150 AS fs60150,
    a.prior_yr_cont_op_amt_fs60175 AS fs60175,
    a.mcaid_cy_cont_ip_amt_fs60200 AS fs60200,
    a.mcaid_cy_cont_op_amt_fs60225 AS fs60225,
    a.champ_cy_cont_ip_amt_fs60300 AS fs60300,
    a.champ_cy_cont_op_amt_fs60325 AS fs60325,
    a.bc_hmo_ppo_disc_ip_amt_fs60400 AS fs60400,
    a.bc_hmo_ppo_disc_op_amt_fs60425 AS fs60425,
    a.mcare_mgd_care_ip_amt_fs60450 AS fs60450,
    a.mcare_mgd_care_op_amt_fs60460 AS fs60460,
    a.mcaid_mgd_care_ip_amt_fs60475 AS fs60475,
    a.mcaid_mgd_care_op_amt_fs60480 AS fs60480,
    a.charity_ip_amt_fs60500 AS fs60500,
    a.charity_op_amt_fs60525 AS fs60525,
    a.other_deduction_ip_amt_fs60600 AS fs60600,
    a.other_deduction_op_amt_fs60625 AS fs60625,
    a.salaries_fdept_620_amt_fs65050 AS fs65050_dept620,
    a.emp_ben_fdept_620_amt_fs65100 AS fs65100_dept620,
    a.bad_debt_amt_fs65500 AS fs65500,
    a.patient_receivable_amt_fs90000 AS fs90000,
    a.reversal_for_05350_amt_fs90008 AS fs90008,
    a.total_patient_rev_amt_fs90230 AS fs90230,
    a.curr_year_cont_amt_fs90245 AS fs90245,
    a.policy_adjustments_amt_fs90246 AS fs90246,
    a.tot_ar_cost_fdept620_gla501000 AS gla501000_dept620,
    a.mcare_clrng_gla110101_110179 AS gla110101_gla110179,
    a.mcare_clrng_gla110189_110197 AS gla110189_gla110197,
    a.mcaid_clrng_gla110201_110279 AS gla110201_gla110279,
    a.mcaid_clrng_gla110286_110297 AS gla110286_gla110297,
    a.credit_refund_amt AS refunds,
    a.reg_accuracy_reg_entry_cnt AS reg_accuracy_entries,
    a.reg_accuracy_reg_changes_cnt AS reg_accuracy_changes,
    a.ins_gt_150_day_pct_amt AS ins_gt_150,
    a.ins_gt_90_day_pct_amt AS ins_gt_90,
    a.write_off_amt,
    CAST(0 as NUMERIC) AS unbilled_bus_ofc,
    CAST(0 as NUMERIC) AS unbilled_med_rec,
    a.health_exchange_ip_amt_fs60490 AS fs60490,
    a.health_exchange_op_amt_fs60495 AS fs60495
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_hospital_level AS a
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS same_store ON same_store.coid = a.coid
     AND same_store.gl_close_ind = 'N'
     AND same_store.month_id = CAST(cw_const as FLOAT64)
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON b.co_id = a.coid
     AND b.company_code = a.company_code
     AND b.user_id = session_user()
;
