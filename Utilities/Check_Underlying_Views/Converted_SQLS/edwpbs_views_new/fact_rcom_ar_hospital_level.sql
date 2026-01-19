-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_ar_hospital_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_rcom_ar_hospital_level AS SELECT
    a.allow_govt_receivable_fs05300,
    a.allow_uncoll_amt_fs05350,
    a.allow_uncoll_nonpat_amtfs05360,
    a.bad_debt_amt_fs65500,
    a.bc_hmo_ppo_disc_ip_amt_fs60400,
    a.bc_hmo_ppo_disc_op_amt_fs60425,
    a.cash_receipt_amt,
    a.champ_cy_cont_ip_amt_fs60300,
    a.champ_cy_cont_op_amt_fs60325,
    a.charity_ip_amt_fs60500,
    a.charity_op_amt_fs60525,
    a.coid,
    a.company_code,
    a.credit_coll_svc_amt_cons83320,
    a.credit_refund_amt,
    a.curr_year_cont_amt_fs90245,
    a.date_sid,
    a.emp_ben_fdept_620_amt_fs65100,
    a.ins_gt_150_day_pct_amt,
    a.ins_gt_90_day_pct_amt,
    a.ip_revenue_routine_amt_fs50100,
    a.ip_rev_ancillary_amt_fs50200,
    a.mcaid_clrng_gla110201_110279,
    a.mcaid_clrng_gla110286_110297,
    a.mcaid_cy_cont_ip_amt_fs60200,
    a.mcaid_cy_cont_op_amt_fs60225,
    a.mcaid_mgd_care_ip_amt_fs60475,
    a.mcaid_mgd_care_op_amt_fs60480,
    a.mcare_clrng_gla110101_110179,
    a.mcare_clrng_gla110189_110197,
    a.mcare_cy_cont_ip_amt_fs60100,
    a.mcare_cy_cont_op_amt_fs60125,
    a.mcare_mgd_care_ip_amt_fs60450,
    a.mcare_mgd_care_op_amt_fs60460,
    a.op_ancillary_rev_amt_fs50400,
    a.other_deduction_ip_amt_fs60600,
    a.other_deduction_op_amt_fs60625,
    a.other_operating_income_fs50900,
    a.patient_receivable_amt_fs90000,
    a.policy_adjustments_amt_fs90246,
    a.prior_yr_cont_ip_amt_fs60150,
    a.prior_yr_cont_op_amt_fs60175,
    a.recoveries_wo_amt_cons65200,
    a.reg_accuracy_reg_changes_cnt,
    a.reg_accuracy_reg_entry_cnt,
    a.reversal_for_05350_amt_fs90008,
    a.salaries_fdept_620_amt_fs65050,
    a.scenario_sid,
    a.source_sid,
    a.total_patient_rev_amt_fs90230,
    a.tot_ar_cost_fdept620_gla501000,
    a.unit_num_sid,
    a.week_of_month_sid,
    a.write_off_amt,
    a.unins_ip_gla508490_gla508499,
    a.unins_op_gla508990_gla508999,
    a.ip_revenue_routine_amt_fs50100 + a.ip_rev_ancillary_amt_fs50200 + a.op_ancillary_rev_amt_fs50400 + a.other_operating_income_fs50900 - a.mcare_cy_cont_ip_amt_fs60100 - a.mcare_cy_cont_op_amt_fs60125 - a.mcaid_cy_cont_ip_amt_fs60200 - a.mcaid_cy_cont_op_amt_fs60225 - a.champ_cy_cont_ip_amt_fs60300 - a.champ_cy_cont_op_amt_fs60325 - a.bc_hmo_ppo_disc_ip_amt_fs60400 - a.bc_hmo_ppo_disc_op_amt_fs60425 - a.mcare_mgd_care_ip_amt_fs60450 - a.mcare_mgd_care_op_amt_fs60460 - a.mcaid_mgd_care_ip_amt_fs60475 - a.mcaid_mgd_care_op_amt_fs60480 - a.charity_ip_amt_fs60500 - a.charity_op_amt_fs60525 - a.other_deduction_ip_amt_fs60600 - a.other_deduction_op_amt_fs60625 - (a.prior_yr_cont_ip_amt_fs60150 + a.prior_yr_cont_op_amt_fs60175) AS net_revenue_fs90260,
    b.days_in_month,
    b.bank_days
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_hospital_level AS a
    INNER JOIN (
      SELECT
          lu_date.month_id,
          sum(lu_date.bank_day_flag) AS bank_days,
          max(lu_date.days_in_month) AS days_in_month
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_date
        GROUP BY 1
    ) AS b ON b.month_id = a.date_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS c ON c.co_id = a.coid
     AND c.company_code = a.company_code
     AND c.user_id = session_user()
;
