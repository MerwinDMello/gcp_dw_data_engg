-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_ar_combined_eis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_ar_combined_eis AS SELECT
    c.date_sid,
    c.scenario_sid,
    40 AS patient_type_sid,
    23 AS patient_financial_class_sid,
    c.unit_num_sid,
    CASE
      WHEN upper(c.company_code) = 'H'
       AND upper(same_store.same_store_ind) = 'Y' THEN 1
      ELSE 0
    END AS same_store_sid,
    c.source_sid,
    20 AS age_month_sid,
    3 AS account_type_sid,
    10 AS account_status_sid,
    1 AS contract_sid,
    24 AS payor_financial_class_sid,
    22 AS product_sid,
    1 AS payor_sid,
    0 AS patient_sid,
    1 AS collection_agency_sid,
    c.week_of_month_sid,
    ROUND(CAST(0 as NUMERIC), 4, 'ROUND_HALF_EVEN') AS ar_day_goals,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS bdc_pct_revenue_goal,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS cash_prcnt_anr_cash_goal,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS cash_receipt,
    ROUND(c.cash_receipt_amt, 3, 'ROUND_HALF_EVEN') AS fm_cash_receipt,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS collected_pct_modeled_rev_goal,
    ROUND(c.recoveries_wo_amt_cons65200, 3, 'ROUND_HALF_EVEN') AS cons65200,
    ROUND(c.credit_coll_svc_amt_cons83320, 3, 'ROUND_HALF_EVEN') AS cons83320,
    ROUND(c.allow_govt_receivable_fs05300, 3, 'ROUND_HALF_EVEN') AS fs05300,
    ROUND(c.allow_uncoll_amt_fs05350, 3, 'ROUND_HALF_EVEN') AS fs05350,
    ROUND(c.allow_uncoll_nonpat_amtfs05360, 3, 'ROUND_HALF_EVEN') AS fs05360,
    ROUND(c.ip_revenue_routine_amt_fs50100, 3, 'ROUND_HALF_EVEN') AS fs50100,
    ROUND(c.ip_rev_ancillary_amt_fs50200, 3, 'ROUND_HALF_EVEN') AS fs50200,
    ROUND(c.op_ancillary_rev_amt_fs50400, 3, 'ROUND_HALF_EVEN') AS fs50400,
    ROUND(c.other_operating_income_fs50900, 3, 'ROUND_HALF_EVEN') AS fs50900,
    ROUND(c.mcare_cy_cont_ip_amt_fs60100, 3, 'ROUND_HALF_EVEN') AS fs60100,
    ROUND(c.mcare_cy_cont_op_amt_fs60125, 3, 'ROUND_HALF_EVEN') AS fs60125,
    ROUND(c.prior_yr_cont_ip_amt_fs60150, 3, 'ROUND_HALF_EVEN') AS fs60150,
    ROUND(c.prior_yr_cont_op_amt_fs60175, 3, 'ROUND_HALF_EVEN') AS fs60175,
    ROUND(c.mcaid_cy_cont_ip_amt_fs60200, 3, 'ROUND_HALF_EVEN') AS fs60200,
    ROUND(c.mcaid_cy_cont_op_amt_fs60225, 3, 'ROUND_HALF_EVEN') AS fs60225,
    ROUND(c.champ_cy_cont_ip_amt_fs60300, 3, 'ROUND_HALF_EVEN') AS fs60300,
    ROUND(c.champ_cy_cont_op_amt_fs60325, 3, 'ROUND_HALF_EVEN') AS fs60325,
    ROUND(c.bc_hmo_ppo_disc_ip_amt_fs60400, 3, 'ROUND_HALF_EVEN') AS fs60400,
    ROUND(c.bc_hmo_ppo_disc_op_amt_fs60425, 3, 'ROUND_HALF_EVEN') AS fs60425,
    ROUND(c.mcare_mgd_care_ip_amt_fs60450, 3, 'ROUND_HALF_EVEN') AS fs60450,
    ROUND(c.mcare_mgd_care_op_amt_fs60460, 3, 'ROUND_HALF_EVEN') AS fs60460,
    ROUND(c.mcaid_mgd_care_ip_amt_fs60475, 3, 'ROUND_HALF_EVEN') AS fs60475,
    ROUND(c.mcaid_mgd_care_op_amt_fs60480, 3, 'ROUND_HALF_EVEN') AS fs60480,
    ROUND(c.charity_ip_amt_fs60500, 3, 'ROUND_HALF_EVEN') AS fs60500,
    ROUND(c.charity_op_amt_fs60525, 3, 'ROUND_HALF_EVEN') AS fs60525,
    ROUND(c.other_deduction_ip_amt_fs60600, 3, 'ROUND_HALF_EVEN') AS fs60600,
    ROUND(c.other_deduction_op_amt_fs60625, 3, 'ROUND_HALF_EVEN') AS fs60625,
    ROUND(c.salaries_fdept_620_amt_fs65050, 3, 'ROUND_HALF_EVEN') AS fs65050_dept620,
    ROUND(c.emp_ben_fdept_620_amt_fs65100, 3, 'ROUND_HALF_EVEN') AS fs65100_dept620,
    ROUND(c.bad_debt_amt_fs65500, 3, 'ROUND_HALF_EVEN') AS fs65500,
    ROUND(c.patient_receivable_amt_fs90000, 3, 'ROUND_HALF_EVEN') AS fs90000,
    ROUND(c.reversal_for_05350_amt_fs90008, 3, 'ROUND_HALF_EVEN') AS fs90008,
    ROUND(c.total_patient_rev_amt_fs90230, 3, 'ROUND_HALF_EVEN') AS fs90230,
    ROUND(c.curr_year_cont_amt_fs90245, 3, 'ROUND_HALF_EVEN') AS fs90245,
    ROUND(c.policy_adjustments_amt_fs90246, 3, 'ROUND_HALF_EVEN') AS fs90246,
    ROUND(c.tot_ar_cost_fdept620_gla501000, 3, 'ROUND_HALF_EVEN') AS gla501000_dept620,
    ROUND(c.mcare_clrng_gla110101_110179, 3, 'ROUND_HALF_EVEN') AS gla110101_gla110179,
    ROUND(c.mcare_clrng_gla110189_110197, 3, 'ROUND_HALF_EVEN') AS gla110189_gla110197,
    ROUND(c.mcaid_clrng_gla110201_110279, 3, 'ROUND_HALF_EVEN') AS gla110201_gla110279,
    ROUND(c.mcaid_clrng_gla110286_110297, 3, 'ROUND_HALF_EVEN') AS gla110286_gla110297,
    ROUND(c.ins_gt_90_day_pct_amt, 5, 'ROUND_HALF_EVEN') AS ins_gt_90_days,
    ROUND(c.ins_gt_150_day_pct_amt, 5, 'ROUND_HALF_EVEN') AS ins_gy_150_days,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS ins_gt_150_days_goal,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS ins_gt_90_days_goal,
    0 AS mc_comp_prcnt_goal,
    ROUND(c.credit_refund_amt, 3, 'ROUND_HALF_EVEN') AS refunds,
    c.reg_accuracy_reg_entry_cnt AS reg_entries,
    c.reg_accuracy_reg_changes_cnt AS reg_changes,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS ctc,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS ctc_per_registration_goal,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS pas_non_overhead_fte,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS registrations,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS unbilled_bus_ofc,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS unbilled_med_rec,
    ROUND(c.unins_ip_gla508490_gla508499, 3, 'ROUND_HALF_EVEN') AS gla508490_gla508499,
    ROUND(c.unins_op_gla508990_gla508999, 3, 'ROUND_HALF_EVEN') AS gla508990_gla508999,
    ROUND(c.health_exchange_ip_amt_fs60490, 3, 'ROUND_HALF_EVEN') AS fs60490,
    ROUND(c.health_exchange_op_amt_fs60495, 3, 'ROUND_HALF_EVEN') AS fs60495,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS gross_revenue,
    c.company_code,
    c.coid
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.fact_rcom_ar_hospital_level AS c
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS same_store ON upper(same_store.coid) = upper(c.coid)
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
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(b.co_id) = upper(c.coid)
     AND upper(b.company_code) = upper(c.company_code)
     AND b.user_id = session_user()
UNION ALL
SELECT
    f.time_sid AS date_sid,
    f.scenario_sid,
    f.patient_type_sid,
    f.patient_financial_class_sid,
    f.unit_num_sid,
    CASE
      WHEN upper(f.company_code) = 'H'
       AND upper(same_store.same_store_ind) = 'Y' THEN 1
      ELSE 0
    END AS same_store_sid,
    f.source_sid,
    20 AS age_month_sid,
    3 AS account_type_sid,
    10 AS account_status_sid,
    1 AS contract_sid,
    24 AS payor_financial_class_sid,
    22 AS product_sid,
    1 AS payor_sid,
    0 AS patient_sid,
    1 AS collection_agency_sid,
    6 AS week_of_month_sid,
    round(ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN'), 4) AS ar_day_goals,
    round(ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN'), 5) AS bdc_pct_revenue_goal,
    round(ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN'), 5) AS cash_prcnt_anr_cash_goal,
    ROUND(f.cash_receipt_amt, 3, 'ROUND_HALF_EVEN') AS cash_receipt,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fm_cash_receipt,
    round(ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN'), 5) AS collected_pct_modeled_rev_goal,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS cons65200,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS cons83320,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs05300,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs05350,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs05360,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs50100,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs50200,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs50400,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs50900,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60100,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60125,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60150,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60175,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60200,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60225,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60300,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60325,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60400,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60425,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60450,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60460,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60475,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60480,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60500,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60525,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60600,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60625,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs65050_dept620,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs65100_dept620,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs65500,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs90000,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs90008,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs90230,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs90245,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs90246,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS gla501000_dept620,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS gla110101_gla110179,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS gla110189_gla110197,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS gla110201_gla110279,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS gla110286_gla110297,
    round(ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN'), 5) AS ins_gt_90_days,
    round(ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN'), 5) AS ins_gy_150_days,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS ins_gt_150_days_goal,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS ins_gt_90_days_goal,
    CAST(ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') as INT64) AS mc_comp_prcnt_goal,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS refunds,
    0 AS reg_entries,
    0 AS reg_changes,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS ctc,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS ctc_per_registration_goal,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS pas_non_overhead_fte,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS registrations,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS unbilled_bus_ofc,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS unbilled_med_rec,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS gla508490_gla508499,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS gla508990_gla508999,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60490,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS fs60495,
    ROUND(f.gross_revenue_amt, 3, 'ROUND_HALF_EVEN') AS gross_revenue,
    f.company_code,
    f.coid
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.fact_rcom_ar_pat_fc_level AS f
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS same_store ON upper(same_store.coid) = upper(f.coid)
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
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(b.co_id) = upper(f.coid)
     AND upper(b.company_code) = upper(f.company_code)
     AND b.user_id = session_user()
UNION ALL
SELECT
    c.date_sid,
    c.scenario_sid,
    40 AS patient_type_sid,
    23 AS patient_financial_class_sid,
    c.unit_num_sid,
    CASE
      WHEN upper(c.company_code) = 'H'
       AND upper(same_store.same_store_ind) = 'Y' THEN 1
      ELSE 0
    END AS same_store_sid,
    c.source_sid,
    20 AS age_month_sid,
    3 AS account_type_sid,
    10 AS account_status_sid,
    1 AS contract_sid,
    24 AS payor_financial_class_sid,
    22 AS product_sid,
    1 AS payor_sid,
    0 AS patient_sid,
    1 AS collection_agency_sid,
    c.week_of_month_sid,
    ROUND(CAST(0 as NUMERIC), 4, 'ROUND_HALF_EVEN') AS ar_day_goals,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS bdc_pct_revenue_goal,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS cash_prcnt_anr_cash_goal,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS cash_receipt,
    ROUND(c.cash_receipt_amt, 3, 'ROUND_HALF_EVEN') AS fm_cash_receipt,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS collected_pct_modeled_rev_goal,
    ROUND(c.recoveries_wo_amt_cons65200, 3, 'ROUND_HALF_EVEN') AS cons65200,
    ROUND(c.credit_coll_svc_amt_cons83320, 3, 'ROUND_HALF_EVEN') AS cons83320,
    ROUND(c.allow_govt_receivable_fs05300, 3, 'ROUND_HALF_EVEN') AS fs05300,
    ROUND(c.allow_uncoll_amt_fs05350, 3, 'ROUND_HALF_EVEN') AS fs05350,
    ROUND(c.allow_uncoll_nonpat_amtfs05360, 3, 'ROUND_HALF_EVEN') AS fs05360,
    ROUND(c.ip_revenue_routine_amt_fs50100, 3, 'ROUND_HALF_EVEN') AS fs50100,
    ROUND(c.ip_rev_ancillary_amt_fs50200, 3, 'ROUND_HALF_EVEN') AS fs50200,
    ROUND(c.op_ancillary_rev_amt_fs50400, 3, 'ROUND_HALF_EVEN') AS fs50400,
    ROUND(c.other_operating_income_fs50900, 3, 'ROUND_HALF_EVEN') AS fs50900,
    ROUND(c.mcare_cy_cont_ip_amt_fs60100, 3, 'ROUND_HALF_EVEN') AS fs60100,
    ROUND(c.mcare_cy_cont_op_amt_fs60125, 3, 'ROUND_HALF_EVEN') AS fs60125,
    ROUND(c.prior_yr_cont_ip_amt_fs60150, 3, 'ROUND_HALF_EVEN') AS fs60150,
    ROUND(c.prior_yr_cont_op_amt_fs60175, 3, 'ROUND_HALF_EVEN') AS fs60175,
    ROUND(c.mcaid_cy_cont_ip_amt_fs60200, 3, 'ROUND_HALF_EVEN') AS fs60200,
    ROUND(c.mcaid_cy_cont_op_amt_fs60225, 3, 'ROUND_HALF_EVEN') AS fs60225,
    ROUND(c.champ_cy_cont_ip_amt_fs60300, 3, 'ROUND_HALF_EVEN') AS fs60300,
    ROUND(c.champ_cy_cont_op_amt_fs60325, 3, 'ROUND_HALF_EVEN') AS fs60325,
    ROUND(c.bc_hmo_ppo_disc_ip_amt_fs60400, 3, 'ROUND_HALF_EVEN') AS fs60400,
    ROUND(c.bc_hmo_ppo_disc_op_amt_fs60425, 3, 'ROUND_HALF_EVEN') AS fs60425,
    ROUND(c.mcare_mgd_care_ip_amt_fs60450, 3, 'ROUND_HALF_EVEN') AS fs60450,
    ROUND(c.mcare_mgd_care_op_amt_fs60460, 3, 'ROUND_HALF_EVEN') AS fs60460,
    ROUND(c.mcaid_mgd_care_ip_amt_fs60475, 3, 'ROUND_HALF_EVEN') AS fs60475,
    ROUND(c.mcaid_mgd_care_op_amt_fs60480, 3, 'ROUND_HALF_EVEN') AS fs60480,
    ROUND(c.charity_ip_amt_fs60500, 3, 'ROUND_HALF_EVEN') AS fs60500,
    ROUND(c.charity_op_amt_fs60525, 3, 'ROUND_HALF_EVEN') AS fs60525,
    ROUND(c.other_deduction_ip_amt_fs60600, 3, 'ROUND_HALF_EVEN') AS fs60600,
    ROUND(c.other_deduction_op_amt_fs60625, 3, 'ROUND_HALF_EVEN') AS fs60625,
    ROUND(c.salaries_fdept_620_amt_fs65050, 3, 'ROUND_HALF_EVEN') AS fs65050_dept620,
    ROUND(c.emp_ben_fdept_620_amt_fs65100, 3, 'ROUND_HALF_EVEN') AS fs65100_dept620,
    ROUND(c.bad_debt_amt_fs65500, 3, 'ROUND_HALF_EVEN') AS fs65500,
    ROUND(c.patient_receivable_amt_fs90000, 3, 'ROUND_HALF_EVEN') AS fs90000,
    ROUND(ROUND(coalesce(c.allow_uncoll_amt_fs05350, CAST(0 as NUMERIC)) + coalesce(c.allow_uncoll_nonpat_amtfs05360, CAST(0 as NUMERIC)), 3, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS fs90008,
    ROUND(ROUND(coalesce(c.ip_revenue_routine_amt_fs50100, CAST(0 as NUMERIC)) + coalesce(c.ip_rev_ancillary_amt_fs50200, CAST(0 as NUMERIC)) + coalesce(c.op_ancillary_rev_amt_fs50400, CAST(0 as NUMERIC)), 3, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS fs90230,
    ROUND(ROUND(coalesce(ROUND(c.mcare_cy_cont_ip_amt_fs60100, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.mcare_cy_cont_op_amt_fs60125, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.prior_yr_cont_ip_amt_fs60150, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.prior_yr_cont_op_amt_fs60175, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.mcaid_cy_cont_ip_amt_fs60200, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.mcaid_cy_cont_op_amt_fs60225, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.champ_cy_cont_ip_amt_fs60300, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.champ_cy_cont_op_amt_fs60325, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)), 3, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS fs90245,
    ROUND(ROUND(coalesce(ROUND(c.bc_hmo_ppo_disc_ip_amt_fs60400, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.bc_hmo_ppo_disc_op_amt_fs60425, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.mcare_mgd_care_ip_amt_fs60450, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.mcare_mgd_care_op_amt_fs60460, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.mcaid_mgd_care_ip_amt_fs60475, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.mcaid_mgd_care_op_amt_fs60480, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.health_exchange_ip_amt_fs60490, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.health_exchange_op_amt_fs60495, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.other_deduction_ip_amt_fs60600, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)) + coalesce(ROUND(c.other_deduction_op_amt_fs60625, 3, 'ROUND_HALF_EVEN'), CAST(0 as NUMERIC)), 3, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS fs90246,
    ROUND(c.tot_ar_cost_fdept620_gla501000, 3, 'ROUND_HALF_EVEN') AS gla501000_dept620,
    ROUND(c.mcare_clrng_gla110101_110179, 3, 'ROUND_HALF_EVEN') AS gla110101_gla110179,
    ROUND(c.mcare_clrng_gla110189_110197, 3, 'ROUND_HALF_EVEN') AS gla110189_gla110197,
    ROUND(c.mcaid_clrng_gla110201_110279, 3, 'ROUND_HALF_EVEN') AS gla110201_gla110279,
    ROUND(c.mcaid_clrng_gla110286_110297, 3, 'ROUND_HALF_EVEN') AS gla110286_gla110297,
    ROUND(c.ins_gt_90_day_pct_amt, 5, 'ROUND_HALF_EVEN') AS ins_gt_90_days,
    ROUND(c.ins_gt_150_day_pct_amt, 5, 'ROUND_HALF_EVEN') AS ins_gy_150_days,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS ins_gt_150_days_goal,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS ins_gt_90_days_goal,
    0 AS mc_comp_prcnt_goal,
    ROUND(c.credit_refund_amt, 3, 'ROUND_HALF_EVEN') AS refunds,
    c.reg_accuracy_reg_entry_cnt AS reg_entries,
    c.reg_accuracy_reg_changes_cnt AS reg_changes,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS ctc,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS ctc_per_registration_goal,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS pas_non_overhead_fte,
    ROUND(CAST(0 as NUMERIC), 5, 'ROUND_HALF_EVEN') AS registrations,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS unbilled_bus_ofc,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS unbilled_med_rec,
    ROUND(c.unins_ip_gla508490_gla508499, 3, 'ROUND_HALF_EVEN') AS gla508490_gla508499,
    ROUND(c.unins_op_gla508990_gla508999, 3, 'ROUND_HALF_EVEN') AS gla508990_gla508999,
    ROUND(c.health_exchange_ip_amt_fs60490, 3, 'ROUND_HALF_EVEN') AS fs60490,
    ROUND(c.health_exchange_op_amt_fs60495, 3, 'ROUND_HALF_EVEN') AS fs60495,
    ROUND(CAST(0 as NUMERIC), 3, 'ROUND_HALF_EVEN') AS gross_revenue,
    c.company_code,
    c.coid
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.fact_rcom_ar_hospital_lvl_clnt AS c
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS same_store ON upper(same_store.coid) = upper(c.coid)
     AND upper(same_store.gl_close_ind) = 'N'
     AND same_store.month_id = CASE
       format_date('%Y%m', CASE
        WHEN extract(DAY from current_date('US/Central')) < 4 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
        ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
      END)
      WHEN '' THEN 0.0
      ELSE CAST(format_date('%Y%m', CASE
        WHEN extract(DAY from current_date('US/Central')) < 4 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
        ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
      END) as FLOAT64)
    END
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(b.co_id) = upper(c.coid)
     AND upper(b.company_code) = upper(c.company_code)
     AND b.user_id = session_user()
;
