-- ------------------------------------------------------------------------------
/***************************************************************************************
   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_bi_integrated_lob AS SELECT
      ref_bi_integrated_lob.integrated_lob_id,
      ref_bi_integrated_lob.integrated_lob_desc,
      ref_bi_integrated_lob.sub_category_desc,
      ref_bi_integrated_lob.process_level_code,
      ref_bi_integrated_lob.dept_code,
      ref_bi_integrated_lob.corrected_dept_code,
      ref_bi_integrated_lob.skill_mix_desc,
      ref_bi_integrated_lob.omit_dept_code,
      ref_bi_integrated_lob.lob_code,
      ref_bi_integrated_lob.functional_dept_desc,
      ref_bi_integrated_lob.match_level_num,
      ref_bi_integrated_lob.recruitment_desc,
      ref_bi_integrated_lob.source_system_code,
      ref_bi_integrated_lob.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_bi_integrated_lob
  ;

