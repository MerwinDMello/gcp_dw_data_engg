CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.consolidated_patient_financial
   OPTIONS(description='Contains financial related data for Hospital encounters.')
  AS SELECT
      consolidated_patient_financial.encounter_id,
      consolidated_patient_financial.encounter_source_system_code,
      consolidated_patient_financial.coid,
      consolidated_patient_financial.company_code,
      consolidated_patient_financial.drg_code_hcfa,
      consolidated_patient_financial.drg_major_diag_cat_code,
      consolidated_patient_financial.drg_medical_surgical_ind,
      consolidated_patient_financial.calculated_length_of_stay_num,
      consolidated_patient_financial.geometric_length_of_stay_num,
      consolidated_patient_financial.drg_payment_weight_amt,
      consolidated_patient_financial.expected_payment_amt,
      consolidated_patient_financial.estimated_net_revenue_amt,
      consolidated_patient_financial.var_contribution_margin_amt,
      consolidated_patient_financial.direct_contribution_margin_amt,
      consolidated_patient_financial.ebdita_amt,
      consolidated_patient_financial.fixed_cost_amt,
      consolidated_patient_financial.variable_cost_amt,
      consolidated_patient_financial.direct_cost_amt,
      consolidated_patient_financial.indirect_cost_amt,
      consolidated_patient_financial.supply_cost_amt,
      consolidated_patient_financial.implant_cost_amt,
      consolidated_patient_financial.other_cost_amt,
      consolidated_patient_financial.salary_cost_amt,
      consolidated_patient_financial.overhead_cost_amt,
      consolidated_patient_financial.payor_payment_amt,
      consolidated_patient_financial.patient_payment_amt,
      consolidated_patient_financial.contractual_allowance_amt,
      consolidated_patient_financial.write_off_amt,
      consolidated_patient_financial.adjustment_amt,
      consolidated_patient_financial.account_balance_amt,
      consolidated_patient_financial.auto_posted_ind,
      consolidated_patient_financial.cleared_account_ind,
      consolidated_patient_financial.dw_last_update_date_time
    FROM
      {{ params.param_cp_base_views_dataset_name }}.consolidated_patient_financial
  ;
