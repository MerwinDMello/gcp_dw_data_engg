-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_ar_hospital_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.fact_rcom_ar_hospital_level AS SELECT
    ROUND(a.allow_govt_receivable_fs05300, 3, 'ROUND_HALF_EVEN') AS allow_govt_receivable_fs05300,
    ROUND(a.allow_uncoll_amt_fs05350, 3, 'ROUND_HALF_EVEN') AS allow_uncoll_amt_fs05350,
    ROUND(a.allow_uncoll_nonpat_amtfs05360, 3, 'ROUND_HALF_EVEN') AS allow_uncoll_nonpat_amtfs05360,
    ROUND(a.bad_debt_amt_fs65500, 3, 'ROUND_HALF_EVEN') AS bad_debt_amt_fs65500,
    ROUND(a.bc_hmo_ppo_disc_ip_amt_fs60400, 3, 'ROUND_HALF_EVEN') AS bc_hmo_ppo_disc_ip_amt_fs60400,
    ROUND(a.bc_hmo_ppo_disc_op_amt_fs60425, 3, 'ROUND_HALF_EVEN') AS bc_hmo_ppo_disc_op_amt_fs60425,
    ROUND(a.cash_receipt_amt, 3, 'ROUND_HALF_EVEN') AS cash_receipt_amt,
    ROUND(a.champ_cy_cont_ip_amt_fs60300, 3, 'ROUND_HALF_EVEN') AS champ_cy_cont_ip_amt_fs60300,
    ROUND(a.champ_cy_cont_op_amt_fs60325, 3, 'ROUND_HALF_EVEN') AS champ_cy_cont_op_amt_fs60325,
    ROUND(a.charity_ip_amt_fs60500, 3, 'ROUND_HALF_EVEN') AS charity_ip_amt_fs60500,
    ROUND(a.charity_op_amt_fs60525, 3, 'ROUND_HALF_EVEN') AS charity_op_amt_fs60525,
    a.coid,
    a.company_code,
    ROUND(a.credit_coll_svc_amt_cons83320, 3, 'ROUND_HALF_EVEN') AS credit_coll_svc_amt_cons83320,
    ROUND(a.credit_refund_amt, 3, 'ROUND_HALF_EVEN') AS credit_refund_amt,
    ROUND(a.curr_year_cont_amt_fs90245, 3, 'ROUND_HALF_EVEN') AS curr_year_cont_amt_fs90245,
    a.date_sid,
    ROUND(a.emp_ben_fdept_620_amt_fs65100, 3, 'ROUND_HALF_EVEN') AS emp_ben_fdept_620_amt_fs65100,
    ROUND(a.ins_gt_150_day_pct_amt, 5, 'ROUND_HALF_EVEN') AS ins_gt_150_day_pct_amt,
    ROUND(a.ins_gt_90_day_pct_amt, 5, 'ROUND_HALF_EVEN') AS ins_gt_90_day_pct_amt,
    ROUND(a.ip_revenue_routine_amt_fs50100, 3, 'ROUND_HALF_EVEN') AS ip_revenue_routine_amt_fs50100,
    ROUND(a.ip_rev_ancillary_amt_fs50200, 3, 'ROUND_HALF_EVEN') AS ip_rev_ancillary_amt_fs50200,
    ROUND(a.mcaid_clrng_gla110201_110279, 3, 'ROUND_HALF_EVEN') AS mcaid_clrng_gla110201_110279,
    ROUND(a.mcaid_clrng_gla110286_110297, 3, 'ROUND_HALF_EVEN') AS mcaid_clrng_gla110286_110297,
    ROUND(a.mcaid_cy_cont_ip_amt_fs60200, 3, 'ROUND_HALF_EVEN') AS mcaid_cy_cont_ip_amt_fs60200,
    ROUND(a.mcaid_cy_cont_op_amt_fs60225, 3, 'ROUND_HALF_EVEN') AS mcaid_cy_cont_op_amt_fs60225,
    ROUND(a.mcaid_mgd_care_ip_amt_fs60475, 3, 'ROUND_HALF_EVEN') AS mcaid_mgd_care_ip_amt_fs60475,
    ROUND(a.mcaid_mgd_care_op_amt_fs60480, 3, 'ROUND_HALF_EVEN') AS mcaid_mgd_care_op_amt_fs60480,
    ROUND(a.mcare_clrng_gla110101_110179, 3, 'ROUND_HALF_EVEN') AS mcare_clrng_gla110101_110179,
    ROUND(a.mcare_clrng_gla110189_110197, 3, 'ROUND_HALF_EVEN') AS mcare_clrng_gla110189_110197,
    ROUND(a.mcare_cy_cont_ip_amt_fs60100, 3, 'ROUND_HALF_EVEN') AS mcare_cy_cont_ip_amt_fs60100,
    ROUND(a.mcare_cy_cont_op_amt_fs60125, 3, 'ROUND_HALF_EVEN') AS mcare_cy_cont_op_amt_fs60125,
    ROUND(a.mcare_mgd_care_ip_amt_fs60450, 3, 'ROUND_HALF_EVEN') AS mcare_mgd_care_ip_amt_fs60450,
    ROUND(a.mcare_mgd_care_op_amt_fs60460, 3, 'ROUND_HALF_EVEN') AS mcare_mgd_care_op_amt_fs60460,
    ROUND(a.op_ancillary_rev_amt_fs50400, 3, 'ROUND_HALF_EVEN') AS op_ancillary_rev_amt_fs50400,
    ROUND(a.other_deduction_ip_amt_fs60600, 3, 'ROUND_HALF_EVEN') AS other_deduction_ip_amt_fs60600,
    ROUND(a.other_deduction_op_amt_fs60625, 3, 'ROUND_HALF_EVEN') AS other_deduction_op_amt_fs60625,
    ROUND(a.other_operating_income_fs50900, 3, 'ROUND_HALF_EVEN') AS other_operating_income_fs50900,
    ROUND(a.patient_receivable_amt_fs90000, 3, 'ROUND_HALF_EVEN') AS patient_receivable_amt_fs90000,
    ROUND(a.policy_adjustments_amt_fs90246, 3, 'ROUND_HALF_EVEN') AS policy_adjustments_amt_fs90246,
    ROUND(a.prior_yr_cont_ip_amt_fs60150, 3, 'ROUND_HALF_EVEN') AS prior_yr_cont_ip_amt_fs60150,
    ROUND(a.prior_yr_cont_op_amt_fs60175, 3, 'ROUND_HALF_EVEN') AS prior_yr_cont_op_amt_fs60175,
    ROUND(a.recoveries_wo_amt_cons65200, 3, 'ROUND_HALF_EVEN') AS recoveries_wo_amt_cons65200,
    a.reg_accuracy_reg_changes_cnt,
    a.reg_accuracy_reg_entry_cnt,
    ROUND(a.reversal_for_05350_amt_fs90008, 3, 'ROUND_HALF_EVEN') AS reversal_for_05350_amt_fs90008,
    ROUND(a.salaries_fdept_620_amt_fs65050, 3, 'ROUND_HALF_EVEN') AS salaries_fdept_620_amt_fs65050,
    a.scenario_sid,
    a.source_sid,
    ROUND(a.total_patient_rev_amt_fs90230, 3, 'ROUND_HALF_EVEN') AS total_patient_rev_amt_fs90230,
    ROUND(a.tot_ar_cost_fdept620_gla501000, 3, 'ROUND_HALF_EVEN') AS tot_ar_cost_fdept620_gla501000,
    a.unit_num_sid,
    a.week_of_month_sid,
    ROUND(a.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
    ROUND(a.unins_ip_gla508490_gla508499, 3, 'ROUND_HALF_EVEN') AS unins_ip_gla508490_gla508499,
    ROUND(a.unins_op_gla508990_gla508999, 3, 'ROUND_HALF_EVEN') AS unins_op_gla508990_gla508999,
    ROUND(a.ip_revenue_routine_amt_fs50100 + a.ip_rev_ancillary_amt_fs50200 + a.op_ancillary_rev_amt_fs50400 + a.other_operating_income_fs50900 - a.mcare_cy_cont_ip_amt_fs60100 - a.mcare_cy_cont_op_amt_fs60125 - a.mcaid_cy_cont_ip_amt_fs60200 - a.mcaid_cy_cont_op_amt_fs60225 - a.champ_cy_cont_ip_amt_fs60300 - a.champ_cy_cont_op_amt_fs60325 - a.bc_hmo_ppo_disc_ip_amt_fs60400 - a.bc_hmo_ppo_disc_op_amt_fs60425 - a.mcare_mgd_care_ip_amt_fs60450 - a.mcare_mgd_care_op_amt_fs60460 - a.mcaid_mgd_care_ip_amt_fs60475 - a.mcaid_mgd_care_op_amt_fs60480 - a.charity_ip_amt_fs60500 - a.charity_op_amt_fs60525 - a.other_deduction_ip_amt_fs60600 - a.other_deduction_op_amt_fs60625 - (a.prior_yr_cont_ip_amt_fs60150 + a.prior_yr_cont_op_amt_fs60175), 3, 'ROUND_HALF_EVEN') AS net_revenue_fs90260,
    b.days_in_month,
    b.bank_days
  FROM
    {{ params.param_pbs_base_views_dataset_name }}.fact_rcom_ar_hospital_level AS a
    INNER JOIN (
      SELECT
          lu_date.month_id,
          sum(lu_date.bank_day_flag) AS bank_days,
          max(lu_date.days_in_month) AS days_in_month
        FROM
          {{ params.param_pbs_base_views_dataset_name }}.lu_date
        GROUP BY 1
    ) AS b ON b.month_id = a.date_sid
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS c ON upper(c.co_id) = upper(a.coid)
     AND upper(c.company_code) = upper(a.company_code)
     AND c.user_id = session_user()
;
