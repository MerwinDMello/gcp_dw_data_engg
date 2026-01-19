CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_esl
   OPTIONS(description='Reference table for the hierarchical descriptions related to the HCA defined Enterprise Service Line.')
  AS SELECT
      ref_esl.esl_code,
      ref_esl.esl_level_1_desc,
      ref_esl.esl_level_2_desc,
      ref_esl.esl_level_3_desc,
      ref_esl.esl_level_4_desc,
      ref_esl.source_system_code,
      ref_esl.dw_last_update_date_time
    FROM
      {{ params.param_pf_base_views_dataset_name }}.ref_esl
  ;
