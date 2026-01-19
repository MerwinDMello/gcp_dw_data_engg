CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.diagnosis_related_group
   OPTIONS(description='A measurement as defined by the Health Care financing Administration used to calculate inpatient service reimbursement.')
  AS SELECT
      diagnosis_related_group.reimbursement_group_code,
      diagnosis_related_group.reimbursement_group_name,
      diagnosis_related_group.reimbursement_group_start_date,
      diagnosis_related_group.drg_payment_weight_amt,
      diagnosis_related_group.reimbursement_group_end_date,
      diagnosis_related_group.drg_desc,
      diagnosis_related_group.drg_major_diag_cat_code,
      diagnosis_related_group.drg_medical_surgical_ind,
      diagnosis_related_group.drg_arithmetic_length_of_stay,
      diagnosis_related_group.drg_geometric_length_of_stay,
      diagnosis_related_group.drg_product_line_code,
      diagnosis_related_group.drg_outlier_length_of_stay,
      diagnosis_related_group.drg_sales_product_line_code,
      diagnosis_related_group.drg_core_coding_report_code,
      diagnosis_related_group.drg_severity_weight_amt2,
      diagnosis_related_group.drg_severity_weight_amt3,
      diagnosis_related_group.drg_severity_weight_amt4,
      diagnosis_related_group.drg_long_desc,
      diagnosis_related_group.cc_mcc_num,
      diagnosis_related_group.drg_tier_num,
      diagnosis_related_group.drg_family_name_num,
      diagnosis_related_group.fy_version,
      diagnosis_related_group.post_acute_drg_ind,
      diagnosis_related_group.special_pay_drg_ind,
      diagnosis_related_group.pepper_med_surg_ind,
      diagnosis_related_group.chois_product_line_code,
      diagnosis_related_group.chois_product_line_desc,
      diagnosis_related_group.dss_advisory_board_sl_text,
      diagnosis_related_group.dss_advisory_board_sub_sl_text,
      diagnosis_related_group.source_system_code,
      diagnosis_related_group.dw_last_update_date_time
    FROM
      {{ params.param_pf_base_views_dataset_name }}.diagnosis_related_group
  ;
