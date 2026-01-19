CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_degree_rank AS SELECT
    ref_degree_rank.degree_rank_level_num,
    ref_degree_rank.subject_code,
    ref_degree_rank.education_code,
    ref_degree_rank.source_system_code,
    ref_degree_rank.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_degree_rank
;
