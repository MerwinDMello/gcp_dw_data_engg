-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_ar_hospital_lvl_eis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_ar_hospital_lvl_eis AS SELECT
    CASE
      WHEN upper(a.company_code) = 'H'
       AND upper(same_store.same_store_ind) = 'Y' THEN 1
      ELSE 0
    END AS same_store_sid,
    a.company_code,
    a.date_sid,
    a.scenario_sid,
    a.unit_num_sid,
    a.week_of_month_sid,
    a.source_sid,
    ROUND(a.cash_receipt_amt, 3, 'ROUND_HALF_EVEN') AS cash_receipts,
    ROUND(a.recoveries_wo_amt_cons65200, 3, 'ROUND_HALF_EVEN') AS cons65200,
    ROUND(a.credit_coll_svc_amt_cons83320, 3, 'ROUND_HALF_EVEN') AS cons83320,
    ROUND(a.allow_govt_receivable_fs05300, 3, 'ROUND_HALF_EVEN') AS fs05300,
    ROUND(a.allow_uncoll_amt_fs05350, 3, 'ROUND_HALF_EVEN') AS fs05350,
    ROUND(a.allow_uncoll_nonpat_amtfs05360, 3, 'ROUND_HALF_EVEN') AS fs05360,
    ROUND(a.ip_revenue_routine_amt_fs50100, 3, 'ROUND_HALF_EVEN') AS fs50100,
    ROUND(a.ip_rev_ancillary_amt_fs50200, 3, 'ROUND_HALF_EVEN') AS fs50200,
    ROUND(a.op_ancillary_rev_amt_fs50400, 3, 'ROUND_HALF_EVEN') AS fs50400,
    ROUND(a.other_operating_income_fs50900, 3, 'ROUND_HALF_EVEN') AS fs50900,
    ROUND(a.mcare_cy_cont_ip_amt_fs60100, 3, 'ROUND_HALF_EVEN') AS fs60100,
    ROUND(a.mcare_cy_cont_op_amt_fs60125, 3, 'ROUND_HALF_EVEN') AS fs60125,
    ROUND(a.prior_yr_cont_ip_amt_fs60150, 3, 'ROUND_HALF_EVEN') AS fs60150,
    ROUND(a.prior_yr_cont_op_amt_fs60175, 3, 'ROUND_HALF_EVEN') AS fs60175,
    ROUND(a.mcaid_cy_cont_ip_amt_fs60200, 3, 'ROUND_HALF_EVEN') AS fs60200,
    ROUND(a.mcaid_cy_cont_op_amt_fs60225, 3, 'ROUND_HALF_EVEN') AS fs60225,
    ROUND(a.champ_cy_cont_ip_amt_fs60300, 3, 'ROUND_HALF_EVEN') AS fs60300,
    ROUND(a.champ_cy_cont_op_amt_fs60325, 3, 'ROUND_HALF_EVEN') AS fs60325,
    ROUND(a.bc_hmo_ppo_disc_ip_amt_fs60400, 3, 'ROUND_HALF_EVEN') AS fs60400,
    ROUND(a.bc_hmo_ppo_disc_op_amt_fs60425, 3, 'ROUND_HALF_EVEN') AS fs60425,
    ROUND(a.mcare_mgd_care_ip_amt_fs60450, 3, 'ROUND_HALF_EVEN') AS fs60450,
    ROUND(a.mcare_mgd_care_op_amt_fs60460, 3, 'ROUND_HALF_EVEN') AS fs60460,
    ROUND(a.mcaid_mgd_care_ip_amt_fs60475, 3, 'ROUND_HALF_EVEN') AS fs60475,
    ROUND(a.mcaid_mgd_care_op_amt_fs60480, 3, 'ROUND_HALF_EVEN') AS fs60480,
    ROUND(a.charity_ip_amt_fs60500, 3, 'ROUND_HALF_EVEN') AS fs60500,
    ROUND(a.charity_op_amt_fs60525, 3, 'ROUND_HALF_EVEN') AS fs60525,
    ROUND(a.other_deduction_ip_amt_fs60600, 3, 'ROUND_HALF_EVEN') AS fs60600,
    ROUND(a.other_deduction_op_amt_fs60625, 3, 'ROUND_HALF_EVEN') AS fs60625,
    ROUND(a.salaries_fdept_620_amt_fs65050, 3, 'ROUND_HALF_EVEN') AS fs65050_dept620,
    ROUND(a.emp_ben_fdept_620_amt_fs65100, 3, 'ROUND_HALF_EVEN') AS fs65100_dept620,
    ROUND(a.bad_debt_amt_fs65500, 3, 'ROUND_HALF_EVEN') AS fs65500,
    ROUND(a.patient_receivable_amt_fs90000, 3, 'ROUND_HALF_EVEN') AS fs90000,
    ROUND(a.reversal_for_05350_amt_fs90008, 3, 'ROUND_HALF_EVEN') AS fs90008,
    ROUND(a.total_patient_rev_amt_fs90230, 3, 'ROUND_HALF_EVEN') AS fs90230,
    ROUND(a.curr_year_cont_amt_fs90245, 3, 'ROUND_HALF_EVEN') AS fs90245,
    ROUND(a.policy_adjustments_amt_fs90246, 3, 'ROUND_HALF_EVEN') AS fs90246,
    ROUND(a.tot_ar_cost_fdept620_gla501000, 3, 'ROUND_HALF_EVEN') AS gla501000_dept620,
    ROUND(a.mcare_clrng_gla110101_110179, 3, 'ROUND_HALF_EVEN') AS gla110101_gla110179,
    ROUND(a.mcare_clrng_gla110189_110197, 3, 'ROUND_HALF_EVEN') AS gla110189_gla110197,
    ROUND(a.mcaid_clrng_gla110201_110279, 3, 'ROUND_HALF_EVEN') AS gla110201_gla110279,
    ROUND(a.mcaid_clrng_gla110286_110297, 3, 'ROUND_HALF_EVEN') AS gla110286_gla110297,
    ROUND(a.credit_refund_amt, 3, 'ROUND_HALF_EVEN') AS refunds,
    a.reg_accuracy_reg_entry_cnt AS reg_accuracy_entries,
    a.reg_accuracy_reg_changes_cnt AS reg_accuracy_changes,
    ROUND(a.ins_gt_150_day_pct_amt, 5, 'ROUND_HALF_EVEN') AS ins_gt_150,
    ROUND(a.ins_gt_90_day_pct_amt, 5, 'ROUND_HALF_EVEN') AS ins_gt_90,
    ROUND(a.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS unbilled_bus_ofc,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS unbilled_med_rec,
    ROUND(a.health_exchange_ip_amt_fs60490, 3, 'ROUND_HALF_EVEN') AS fs60490,
    ROUND(a.health_exchange_op_amt_fs60495, 3, 'ROUND_HALF_EVEN') AS fs60495
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.fact_rcom_ar_hospital_level AS a
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS same_store ON upper(same_store.coid) = upper(a.coid)
     AND upper(same_store.gl_close_ind) = 'N'
     AND same_store.month_id = CASE
       format_date('%Y%m', CASE
        WHEN extract(DAY from current_date('US/Central')) < 6 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
        ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
      END)
      WHEN '' THEN 0.0
      ELSE CAST(format_date('%Y%m', CASE
        WHEN extract(DAY from current_date('US/Central')) < 6 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
        ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
      END) as FLOAT64)
    END
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(b.co_id) = upper(a.coid)
     AND upper(b.company_code) = upper(a.company_code)
     AND b.user_id = session_user()
UNION ALL
SELECT
    CASE
      WHEN upper(a.company_code) = 'H'
       AND upper(same_store.same_store_ind) = 'Y' THEN 1
      ELSE 0
    END AS same_store_sid,
    a.company_code,
    a.date_sid,
    a.scenario_sid,
    a.unit_num_sid,
    a.week_of_month_sid,
    a.source_sid,
    ROUND(a.cash_receipt_amt, 3, 'ROUND_HALF_EVEN') AS cash_receipts,
    ROUND(a.recoveries_wo_amt_cons65200, 3, 'ROUND_HALF_EVEN') AS cons65200,
    ROUND(a.credit_coll_svc_amt_cons83320, 3, 'ROUND_HALF_EVEN') AS cons83320,
    ROUND(a.allow_govt_receivable_fs05300, 3, 'ROUND_HALF_EVEN') AS fs05300,
    ROUND(a.allow_uncoll_amt_fs05350, 3, 'ROUND_HALF_EVEN') AS fs05350,
    ROUND(a.allow_uncoll_nonpat_amtfs05360, 3, 'ROUND_HALF_EVEN') AS fs05360,
    ROUND(a.ip_revenue_routine_amt_fs50100, 3, 'ROUND_HALF_EVEN') AS fs50100,
    ROUND(a.ip_rev_ancillary_amt_fs50200, 3, 'ROUND_HALF_EVEN') AS fs50200,
    ROUND(a.op_ancillary_rev_amt_fs50400, 3, 'ROUND_HALF_EVEN') AS fs50400,
    ROUND(a.other_operating_income_fs50900, 3, 'ROUND_HALF_EVEN') AS fs50900,
    ROUND(a.mcare_cy_cont_ip_amt_fs60100, 3, 'ROUND_HALF_EVEN') AS fs60100,
    ROUND(a.mcare_cy_cont_op_amt_fs60125, 3, 'ROUND_HALF_EVEN') AS fs60125,
    ROUND(a.prior_yr_cont_ip_amt_fs60150, 3, 'ROUND_HALF_EVEN') AS fs60150,
    ROUND(a.prior_yr_cont_op_amt_fs60175, 3, 'ROUND_HALF_EVEN') AS fs60175,
    ROUND(a.mcaid_cy_cont_ip_amt_fs60200, 3, 'ROUND_HALF_EVEN') AS fs60200,
    ROUND(a.mcaid_cy_cont_op_amt_fs60225, 3, 'ROUND_HALF_EVEN') AS fs60225,
    ROUND(a.champ_cy_cont_ip_amt_fs60300, 3, 'ROUND_HALF_EVEN') AS fs60300,
    ROUND(a.champ_cy_cont_op_amt_fs60325, 3, 'ROUND_HALF_EVEN') AS fs60325,
    ROUND(a.bc_hmo_ppo_disc_ip_amt_fs60400, 3, 'ROUND_HALF_EVEN') AS fs60400,
    ROUND(a.bc_hmo_ppo_disc_op_amt_fs60425, 3, 'ROUND_HALF_EVEN') AS fs60425,
    ROUND(a.mcare_mgd_care_ip_amt_fs60450, 3, 'ROUND_HALF_EVEN') AS fs60450,
    ROUND(a.mcare_mgd_care_op_amt_fs60460, 3, 'ROUND_HALF_EVEN') AS fs60460,
    ROUND(a.mcaid_mgd_care_ip_amt_fs60475, 3, 'ROUND_HALF_EVEN') AS fs60475,
    ROUND(a.mcaid_mgd_care_op_amt_fs60480, 3, 'ROUND_HALF_EVEN') AS fs60480,
    ROUND(a.charity_ip_amt_fs60500, 3, 'ROUND_HALF_EVEN') AS fs60500,
    ROUND(a.charity_op_amt_fs60525, 3, 'ROUND_HALF_EVEN') AS fs60525,
    ROUND(a.other_deduction_ip_amt_fs60600, 3, 'ROUND_HALF_EVEN') AS fs60600,
    ROUND(a.other_deduction_op_amt_fs60625, 3, 'ROUND_HALF_EVEN') AS fs60625,
    ROUND(a.salaries_fdept_620_amt_fs65050, 3, 'ROUND_HALF_EVEN') AS fs65050_dept620,
    ROUND(a.emp_ben_fdept_620_amt_fs65100, 3, 'ROUND_HALF_EVEN') AS fs65100_dept620,
    ROUND(a.bad_debt_amt_fs65500, 3, 'ROUND_HALF_EVEN') AS fs65500,
    ROUND(a.patient_receivable_amt_fs90000, 3, 'ROUND_HALF_EVEN') AS fs90000,
    ROUND(a.reversal_for_05350_amt_fs90008, 3, 'ROUND_HALF_EVEN') AS fs90008,
    ROUND(a.total_patient_rev_amt_fs90230, 3, 'ROUND_HALF_EVEN') AS fs90230,
    ROUND(a.curr_year_cont_amt_fs90245, 3, 'ROUND_HALF_EVEN') AS fs90245,
    ROUND(a.policy_adjustments_amt_fs90246, 3, 'ROUND_HALF_EVEN') AS fs90246,
    ROUND(a.tot_ar_cost_fdept620_gla501000, 3, 'ROUND_HALF_EVEN') AS gla501000_dept620,
    ROUND(a.mcare_clrng_gla110101_110179, 3, 'ROUND_HALF_EVEN') AS gla110101_gla110179,
    ROUND(a.mcare_clrng_gla110189_110197, 3, 'ROUND_HALF_EVEN') AS gla110189_gla110197,
    ROUND(a.mcaid_clrng_gla110201_110279, 3, 'ROUND_HALF_EVEN') AS gla110201_gla110279,
    ROUND(a.mcaid_clrng_gla110286_110297, 3, 'ROUND_HALF_EVEN') AS gla110286_gla110297,
    ROUND(a.credit_refund_amt, 3, 'ROUND_HALF_EVEN') AS refunds,
    a.reg_accuracy_reg_entry_cnt AS reg_accuracy_entries,
    a.reg_accuracy_reg_changes_cnt AS reg_accuracy_changes,
    ROUND(a.ins_gt_150_day_pct_amt, 5, 'ROUND_HALF_EVEN') AS ins_gt_150,
    ROUND(a.ins_gt_90_day_pct_amt, 5, 'ROUND_HALF_EVEN') AS ins_gt_90,
    ROUND(a.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS unbilled_bus_ofc,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS unbilled_med_rec,
    ROUND(a.health_exchange_ip_amt_fs60490, 3, 'ROUND_HALF_EVEN') AS fs60490,
    ROUND(a.health_exchange_op_amt_fs60495, 3, 'ROUND_HALF_EVEN') AS fs60495
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_hospital_level AS a
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS same_store ON upper(same_store.coid) = upper(a.coid)
     AND upper(same_store.gl_close_ind) = 'N'
     AND same_store.month_id = CASE
       format_date('%Y%m', CASE
        WHEN extract(DAY from current_date('US/Central')) < 6 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
        ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
      END)
      WHEN '' THEN 0.0
      ELSE CAST(format_date('%Y%m', CASE
        WHEN extract(DAY from current_date('US/Central')) < 6 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
        ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
      END) as FLOAT64)
    END
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(b.co_id) = upper(a.coid)
     AND upper(b.company_code) = upper(a.company_code)
     AND b.user_id = session_user()
;
