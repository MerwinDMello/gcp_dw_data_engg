-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_dcrp_tmp.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_dcrp_tmp AS SELECT
    a.date_sid,
    a.patient_dw_id,
    a.payor_dw_id,
    a.iplan_insurance_order_num,
    a.eor_log_date,
    a.log_id,
    a.ar_bill_thru_date,
    a.log_sequence_num,
    a.discrepancy_creation_date,
    a.year_created_sid,
    p.year_created_sid AS cost_year_sid,
    a.patient_sid,
    a.payor_financial_class_sid,
    a.payor_sid,
    a.payor_sequence_sid,
    a.patient_type_sid,
    a.unit_num_sid,
    a.discrepancy_age_month_sid,
    a.discharge_age_month_sid,
    a.scenario_sid,
    a.reason_code_1_sid,
    a.reason_code_2_sid,
    a.reason_code_3_sid,
    a.reason_code_4_sid,
    a.source_sid,
    a.coid,
    a.company_code,
    a.resolve_reason_code,
    a.discharge_date,
    a.ra_remit_date,
    a.reason_assignment_date_1,
    a.reason_assignment_date_2,
    a.reason_assignment_date_3,
    a.reason_assignment_date_4,
    a.account_balance_amt,
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
    a.var_gross_rbmt_beg_amt AS var_beg_amt,
    a.var_gross_rbmt_new_amt AS var_new_amt,
    a.var_gross_rbmt_resolved_amt AS var_resolved_amt,
    a.var_gross_rbmt_othr_cor_amt AS var_othr_cor_amt,
    a.var_gross_rbmt_end_amt AS var_end_amt,
    a.exp_gross_rbmt_crnt_amt AS exp_crnt_amt,
    a.exp_gross_rbmt_rate_amt AS exp_rate_amt,
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
  WHERE a.date_sid < 201310
UNION ALL
SELECT
    a.date_sid,
    a.patient_dw_id,
    a.payor_dw_id,
    a.iplan_insurance_order_num,
    a.eor_log_date,
    a.log_id,
    a.ar_bill_thru_date,
    a.log_sequence_num,
    a.discrepancy_creation_date,
    a.year_created_sid,
    p.year_created_sid AS cost_year_sid,
    a.patient_sid,
    a.payor_financial_class_sid,
    a.payor_sid,
    a.payor_sequence_sid,
    a.patient_type_sid,
    a.unit_num_sid,
    a.discrepancy_age_month_sid,
    a.discharge_age_month_sid,
    a.scenario_sid,
    a.reason_code_1_sid,
    a.reason_code_2_sid,
    a.reason_code_3_sid,
    a.reason_code_4_sid,
    a.source_sid,
    a.coid,
    a.company_code,
    a.resolve_reason_code,
    a.discharge_date,
    a.ra_remit_date,
    a.reason_assignment_date_1,
    a.reason_assignment_date_2,
    a.reason_assignment_date_3,
    a.reason_assignment_date_4,
    a.account_balance_amt,
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
    a.var_cont_alw_beg_amt AS var_beg_amt,
    a.var_cont_alw_new_amt AS var_new_amt,
    a.var_cont_alw_resolved_amt AS var_resolved_amt,
    a.var_cont_alw_othr_cor_amt AS var_othr_cor_amt,
    a.var_cont_alw_end_amt AS var_end_amt,
    a.exp_cont_alw_crnt_amt AS exp_crnt_amt,
    a.exp_cont_alw_rate_amt AS exp_rate_amt,
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
     AND a.discrepancy_creation_date BETWEEN p.cost_year_beg_date AND p.cost_year_end_date
UNION ALL
SELECT
    a.date_sid,
    a.patient_dw_id,
    a.payor_dw_id,
    a.iplan_insurance_order_num,
    a.eor_log_date,
    a.log_id,
    a.ar_bill_thru_date,
    a.log_sequence_num,
    a.discrepancy_creation_date,
    a.year_created_sid,
    p.year_created_sid AS cost_year_sid,
    a.patient_sid,
    a.payor_financial_class_sid,
    a.payor_sid,
    a.payor_sequence_sid,
    a.patient_type_sid,
    a.unit_num_sid,
    a.discrepancy_age_month_sid,
    a.discharge_age_month_sid,
    a.scenario_sid,
    a.reason_code_1_sid,
    a.reason_code_2_sid,
    a.reason_code_3_sid,
    a.reason_code_4_sid,
    a.source_sid,
    a.coid,
    a.company_code,
    a.resolve_reason_code,
    a.discharge_date,
    a.ra_remit_date,
    a.reason_assignment_date_1,
    a.reason_assignment_date_2,
    a.reason_assignment_date_3,
    a.reason_assignment_date_4,
    a.account_balance_amt,
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
    a.var_payment_beg_amt AS var_beg_amt,
    a.var_payment_new_amt AS var_new_amt,
    a.var_payment_resolved_amt AS var_resolved_amt,
    a.var_payment_othr_cor_amt AS var_othr_cor_amt,
    a.var_payment_end_amt AS var_end_amt,
    a.exp_payment_crnt_amt AS exp_crnt_amt,
    a.exp_payment_rate_amt AS exp_rate_amt,
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
     AND a.discrepancy_creation_date BETWEEN p.cost_year_beg_date AND p.cost_year_end_date
  WHERE a.date_sid > 201310
UNION ALL
SELECT
    a.date_sid,
    a.patient_dw_id,
    a.payor_dw_id,
    a.iplan_insurance_order_num,
    a.eor_log_date,
    a.log_id,
    a.ar_bill_thru_date,
    a.log_sequence_num,
    a.discrepancy_creation_date,
    a.year_created_sid,
    p.year_created_sid AS cost_year_sid,
    a.patient_sid,
    a.payor_financial_class_sid,
    a.payor_sid,
    a.payor_sequence_sid,
    a.patient_type_sid,
    a.unit_num_sid,
    a.discrepancy_age_month_sid,
    a.discharge_age_month_sid,
    a.scenario_sid,
    a.reason_code_1_sid,
    a.reason_code_2_sid,
    a.reason_code_3_sid,
    a.reason_code_4_sid,
    a.source_sid,
    a.coid,
    a.company_code,
    a.resolve_reason_code,
    a.discharge_date,
    a.ra_remit_date,
    a.reason_assignment_date_1,
    a.reason_assignment_date_2,
    a.reason_assignment_date_3,
    a.reason_assignment_date_4,
    a.account_balance_amt,
    a.primary_reason_code_change_cnt,
    a.discrepancy_days,
    a.discrepancy_resolved_date,
    'CHV' AS discrepany_type_code,
    CASE
      WHEN a.charge_dcrp_type_sid = 3 THEN 7
    END AS discrepancy_type_sid,
    a.charge_end_strf_sid AS dollar_strtf_sid,
    a.var_charge_beg_amt AS var_beg_amt,
    a.var_charge_new_amt AS var_new_amt,
    a.var_charge_resolved_amt AS var_resolved_amt,
    a.var_charge_othr_cor_amt AS var_othr_cor_amt,
    a.var_charge_end_amt AS var_end_amt,
    a.exp_charge_crnt_amt AS exp_crnt_amt,
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
     AND a.discrepancy_creation_date BETWEEN p.cost_year_beg_date AND p.cost_year_end_date
;
