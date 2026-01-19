CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_ajcc_stage
   OPTIONS(description='This table contains American Joint Committee On Cancer Stage codes and descriptions')
  AS SELECT
      ref_ajcc_stage.ajcc_stage_id,
      ref_ajcc_stage.ajcc_stage_code,
      ref_ajcc_stage.ajcc_stage_sub_code,
      ref_ajcc_stage.ajcc_stage_desc,
      ref_ajcc_stage.ajcc_stage_group_id,
      ref_ajcc_stage.source_system_code,
      ref_ajcc_stage.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_ajcc_stage
  ;
