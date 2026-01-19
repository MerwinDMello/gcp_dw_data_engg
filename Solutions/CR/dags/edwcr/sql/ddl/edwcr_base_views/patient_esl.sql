CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.patient_esl
   OPTIONS(description='Table to store the Enterprise Service Line for each Patient_DW_ID')
  AS SELECT
      patient_esl.patient_dw_id,
      patient_esl.esl_code,
      patient_esl.esl_level_5_code,
      patient_esl.esl_level_5_desc,
      patient_esl.coid,
      patient_esl.company_code,
      patient_esl.pat_acct_num,
      patient_esl.chois_product_line_code,
      patient_esl.patient_type_summary_code,
      patient_esl.drg_hcfa_version_code,
      patient_esl.dss_op_cpt_hier,
      patient_esl.source_system_code,
      patient_esl.dw_last_update_date_time
    FROM
      {{ params.param_pf_base_views_dataset_name }}.patient_esl
  ;
