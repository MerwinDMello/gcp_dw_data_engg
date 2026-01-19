-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_dcrp_all_gvchg.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_dcrp_all_gvchg AS SELECT
    a.date_sid,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    a.iplan_insurance_order_num,
    a.eor_log_date,
    a.log_id,
    a.ar_bill_thru_date,
    a.log_sequence_num,
    a.discrepancy_creation_date,
    p.year_created_sid,
    b.year_created_sid AS cost_year_sid,
    b.cost_year_end_date,
    ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
    a.payor_financial_class_sid,
    a.payor_sid,
    a.payor_sequence_sid,
    a.patient_type_sid,
    a.unit_num_sid,
    a.discrepancy_age_month_sid,
    a.discharge_age_month_sid,
    a.scenario_sid,
    ROUND(a.reason_code_1_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_1_sid,
    ROUND(a.reason_code_2_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_2_sid,
    ROUND(a.reason_code_3_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_3_sid,
    ROUND(a.reason_code_4_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_4_sid,
    a.source_sid,
    ROUND(a.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id,
    a.coid,
    a.company_code,
    a.resolve_reason_code,
    a.discharge_date,
    a.ra_remit_date,
    a.reason_assignment_date_1,
    a.reason_assignment_date_2,
    a.reason_assignment_date_3,
    a.reason_assignment_date_4,
    ROUND(a.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
    a.primary_reason_code_change_cnt,
    a.discrepancy_days,
    a.discrepancy_resolved_date,
    'PYV' AS discrepany_type_code,
    CASE
       a.gross_rbmt_dcrp_type_sid
      WHEN 1 THEN 2
      WHEN 2 THEN 3
      WHEN 3 THEN 1
      ELSE 0
    END AS discrepancy_type_sid,
    a.gross_rbmt_end_strf_sid AS dollar_strtf_sid,
    ROUND(a.var_gross_rbmt_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_beg_amt,
    ROUND(a.var_gross_rbmt_new_amt, 3, 'ROUND_HALF_EVEN') AS var_new_amt,
    ROUND(a.var_gross_rbmt_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_resolved_amt,
    ROUND(a.var_gross_rbmt_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_othr_cor_amt,
    ROUND(a.var_gross_rbmt_end_amt, 3, 'ROUND_HALF_EVEN') AS var_end_amt,
    ROUND(a.exp_gross_rbmt_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_crnt_amt,
    ROUND(a.exp_gross_rbmt_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_rate_amt,
    a.var_gross_rbmt_beg_cnt AS var_beg_cnt,
    a.var_gross_rbmt_new_cnt AS var_new_cnt,
    a.var_gross_rbmt_resolved_cnt AS var_resolved_cnt,
    a.var_gross_rbmt_othr_cor_cnt AS var_othr_cor_cnt,
    a.var_gross_rbmt_end_cnt AS var_end_cnt,
    a.exp_gross_rbmt_crnt_cnt AS exp_crnt_cnt,
    a.exp_gross_rbmt_rate_cnt AS exp_rate_cnt,
    CASE
      WHEN a.var_gross_rbmt_resolved_cnt <> 0 THEN a.discrepancy_days
      ELSE 0
    END AS resolved_days,
    CASE
      WHEN a.var_gross_rbmt_end_cnt <> 0 THEN a.discrepancy_days
      ELSE 0
    END AS end_days
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_discrepancy AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS p ON a.unit_num_sid = p.unit_num_sid
     AND a.source_sid = p.source_sid
     AND a.discrepancy_creation_date BETWEEN p.cost_year_beg_date AND p.cost_year_end_date
     AND p.source_sid IN(
      6, 11
    )
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS b ON a.unit_num_sid = b.unit_num_sid
     AND a.source_sid = b.source_sid
     AND CASE
      WHEN a.ar_bill_thru_date = DATE '0001-01-01' THEN a.discharge_date
      ELSE a.ar_bill_thru_date
    END BETWEEN b.cost_year_beg_date AND b.cost_year_end_date
  WHERE a.date_sid <= 201310
UNION ALL
SELECT
    a.date_sid,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    a.iplan_insurance_order_num,
    a.eor_log_date,
    a.log_id,
    a.ar_bill_thru_date,
    a.log_sequence_num,
    a.discrepancy_creation_date,
    p.year_created_sid,
    b.year_created_sid AS cost_year_sid,
    b.cost_year_end_date,
    ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
    a.payor_financial_class_sid,
    a.payor_sid,
    a.payor_sequence_sid,
    a.patient_type_sid,
    a.unit_num_sid,
    a.discrepancy_age_month_sid,
    a.discharge_age_month_sid,
    a.scenario_sid,
    ROUND(a.reason_code_1_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_1_sid,
    ROUND(a.reason_code_2_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_2_sid,
    ROUND(a.reason_code_3_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_3_sid,
    ROUND(a.reason_code_4_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_4_sid,
    a.source_sid,
    ROUND(a.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id,
    a.coid,
    a.company_code,
    a.resolve_reason_code,
    a.discharge_date,
    a.ra_remit_date,
    a.reason_assignment_date_1,
    a.reason_assignment_date_2,
    a.reason_assignment_date_3,
    a.reason_assignment_date_4,
    ROUND(a.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
    a.primary_reason_code_change_cnt,
    a.discrepancy_days,
    a.discrepancy_resolved_date,
    'CAV' AS discrepany_type_code,
    CASE
       a.cont_alw_dcrp_type_sid
      WHEN 1 THEN 5
      WHEN 2 THEN 6
      WHEN 3 THEN 4
      ELSE 0
    END AS discrepancy_type_sid,
    a.cont_alw_end_strf_sid AS dollar_strtf_sid,
    ROUND(a.var_cont_alw_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_beg_amt,
    ROUND(a.var_cont_alw_new_amt, 3, 'ROUND_HALF_EVEN') AS var_new_amt,
    ROUND(a.var_cont_alw_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_resolved_amt,
    ROUND(a.var_cont_alw_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_othr_cor_amt,
    ROUND(a.var_cont_alw_end_amt, 3, 'ROUND_HALF_EVEN') AS var_end_amt,
    ROUND(a.exp_cont_alw_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_crnt_amt,
    ROUND(a.exp_cont_alw_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_rate_amt,
    a.var_cont_alw_beg_cnt AS var_beg_cnt,
    a.var_cont_alw_new_cnt AS var_new_cnt,
    a.var_cont_alw_resolved_cnt AS var_resolved_cnt,
    a.var_cont_alw_othr_cor_cnt AS var_othr_cor_cnt,
    a.var_cont_alw_end_cnt AS var_end_cnt,
    a.exp_cont_alw_crnt_cnt AS exp_crnt_cnt,
    a.exp_cont_alw_rate_cnt AS exp_rate_cnt,
    CASE
      WHEN a.var_cont_alw_resolved_cnt <> 0 THEN a.discrepancy_days
      ELSE 0
    END AS resolved_days,
    CASE
      WHEN a.var_cont_alw_end_cnt <> 0 THEN a.discrepancy_days
      ELSE 0
    END AS end_days
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_discrepancy AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS p ON a.unit_num_sid = p.unit_num_sid
     AND a.source_sid = p.source_sid
     AND p.source_sid IN(
      6, 11
    )
     AND a.discrepancy_creation_date BETWEEN p.cost_year_beg_date AND p.cost_year_end_date
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS b ON a.unit_num_sid = b.unit_num_sid
     AND a.source_sid = b.source_sid
     AND CASE
      WHEN a.ar_bill_thru_date = DATE '0001-01-01' THEN a.discharge_date
      ELSE a.ar_bill_thru_date
    END BETWEEN b.cost_year_beg_date AND b.cost_year_end_date
UNION ALL
SELECT
    a.date_sid,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    a.iplan_insurance_order_num,
    a.eor_log_date,
    a.log_id,
    a.ar_bill_thru_date,
    a.log_sequence_num,
    a.discrepancy_creation_date,
    p.year_created_sid,
    b.year_created_sid AS cost_year_sid,
    b.cost_year_end_date,
    ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
    a.payor_financial_class_sid,
    a.payor_sid,
    a.payor_sequence_sid,
    a.patient_type_sid,
    a.unit_num_sid,
    a.discrepancy_age_month_sid,
    a.discharge_age_month_sid,
    a.scenario_sid,
    ROUND(a.reason_code_1_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_1_sid,
    ROUND(a.reason_code_2_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_2_sid,
    ROUND(a.reason_code_3_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_3_sid,
    ROUND(a.reason_code_4_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_4_sid,
    a.source_sid,
    ROUND(a.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id,
    a.coid,
    a.company_code,
    a.resolve_reason_code,
    a.discharge_date,
    a.ra_remit_date,
    a.reason_assignment_date_1,
    a.reason_assignment_date_2,
    a.reason_assignment_date_3,
    a.reason_assignment_date_4,
    ROUND(a.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
    a.primary_reason_code_change_cnt,
    a.discrepancy_days,
    a.discrepancy_resolved_date,
    'PYV' AS discrepany_type_code,
    CASE
       a.payment_dcrp_type_sid
      WHEN 1 THEN 2
      WHEN 2 THEN 3
      WHEN 3 THEN 1
      ELSE 0
    END AS discrepancy_type_sid,
    a.payment_end_strf_sid AS dollar_strtf_sid,
    ROUND(a.var_payment_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_beg_amt,
    ROUND(a.var_payment_new_amt, 3, 'ROUND_HALF_EVEN') AS var_new_amt,
    ROUND(a.var_payment_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_resolved_amt,
    ROUND(a.var_payment_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_othr_cor_amt,
    ROUND(a.var_payment_end_amt, 3, 'ROUND_HALF_EVEN') AS var_end_amt,
    ROUND(a.exp_payment_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_crnt_amt,
    ROUND(a.exp_payment_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_rate_amt,
    a.var_payment_beg_cnt AS var_beg_cnt,
    a.var_payment_new_cnt AS var_new_cnt,
    a.var_payment_resolved_cnt AS var_resolved_cnt,
    a.var_payment_othr_cor_cnt AS var_othr_cor_cnt,
    a.var_payment_end_cnt AS var_end_cnt,
    a.exp_payment_crnt_cnt AS exp_crnt_cnt,
    a.exp_payment_rate_cnt AS exp_rate_cnt,
    CASE
      WHEN a.var_payment_resolved_cnt <> 0 THEN a.discrepancy_days
      ELSE 0
    END AS resolved_days,
    CASE
      WHEN a.var_payment_end_cnt <> 0 THEN a.discrepancy_days
      ELSE 0
    END AS end_days
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_discrepancy AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS p ON a.unit_num_sid = p.unit_num_sid
     AND a.source_sid = p.source_sid
     AND p.source_sid IN(
      6, 11
    )
     AND a.discrepancy_creation_date BETWEEN p.cost_year_beg_date AND p.cost_year_end_date
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS b ON a.unit_num_sid = b.unit_num_sid
     AND a.source_sid = b.source_sid
     AND CASE
      WHEN a.ar_bill_thru_date = DATE '0001-01-01' THEN a.discharge_date
      ELSE a.ar_bill_thru_date
    END BETWEEN b.cost_year_beg_date AND b.cost_year_end_date
  WHERE a.date_sid > 201310
   OR a.date_sid <= 201310
   AND a.payor_financial_class_sid = 1
   AND a.source_sid = 7
UNION ALL
SELECT
    a.date_sid,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    a.iplan_insurance_order_num,
    a.eor_log_date,
    a.log_id,
    a.ar_bill_thru_date,
    a.log_sequence_num,
    a.discrepancy_creation_date,
    p.year_created_sid,
    b.year_created_sid AS cost_year_sid,
    b.cost_year_end_date,
    ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
    a.payor_financial_class_sid,
    a.payor_sid,
    a.payor_sequence_sid,
    a.patient_type_sid,
    a.unit_num_sid,
    a.discrepancy_age_month_sid,
    a.discharge_age_month_sid,
    a.scenario_sid,
    ROUND(a.reason_code_1_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_1_sid,
    ROUND(a.reason_code_2_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_2_sid,
    ROUND(a.reason_code_3_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_3_sid,
    ROUND(a.reason_code_4_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_4_sid,
    a.source_sid,
    ROUND(a.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id,
    a.coid,
    a.company_code,
    a.resolve_reason_code,
    a.discharge_date,
    a.ra_remit_date,
    a.reason_assignment_date_1,
    a.reason_assignment_date_2,
    a.reason_assignment_date_3,
    a.reason_assignment_date_4,
    ROUND(a.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
    a.primary_reason_code_change_cnt,
    a.discrepancy_days,
    a.discrepancy_resolved_date,
    'CHV' AS discrepany_type_code,
    CASE
      WHEN a.charge_dcrp_type_sid = 3 THEN 7
    END AS discrepancy_type_sid,
    a.charge_end_strf_sid AS dollar_strtf_sid,
    ROUND(a.var_charge_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_beg_amt,
    ROUND(a.var_charge_new_amt, 3, 'ROUND_HALF_EVEN') AS var_new_amt,
    ROUND(a.var_charge_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_resolved_amt,
    ROUND(a.var_charge_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_othr_cor_amt,
    ROUND(a.var_charge_end_amt, 3, 'ROUND_HALF_EVEN') AS var_end_amt,
    ROUND(a.exp_charge_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_crnt_amt,
    CAST(NULL as NUMERIC) AS exp_rate_amt,
    a.var_charge_beg_cnt AS var_beg_cnt,
    a.var_charge_new_cnt AS var_new_cnt,
    a.var_charge_resolved_cnt AS var_resolved_cnt,
    a.var_charge_othr_cor_cnt AS var_othr_cor_cnt,
    a.var_charge_end_cnt AS var_end_cnt,
    a.exp_charge_crnt_cnt AS exp_crnt_cnt,
    CAST(NULL as INT64) AS exp_rate_cnt,
    CASE
      WHEN a.var_charge_resolved_cnt <> 0 THEN a.discrepancy_days
      ELSE 0
    END AS resolved_days,
    CASE
      WHEN a.var_charge_end_cnt <> 0 THEN a.discrepancy_days
      ELSE 0
    END AS end_days
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_discrepancy AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS p ON a.unit_num_sid = p.unit_num_sid
     AND a.source_sid = p.source_sid
     AND p.source_sid IN(
      6, 11
    )
     AND a.discrepancy_creation_date BETWEEN p.cost_year_beg_date AND p.cost_year_end_date
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS b ON a.unit_num_sid = b.unit_num_sid
     AND a.source_sid = b.source_sid
     AND CASE
      WHEN a.ar_bill_thru_date = DATE '0001-01-01' THEN a.discharge_date
      ELSE a.ar_bill_thru_date
    END BETWEEN b.cost_year_beg_date AND b.cost_year_end_date
;
