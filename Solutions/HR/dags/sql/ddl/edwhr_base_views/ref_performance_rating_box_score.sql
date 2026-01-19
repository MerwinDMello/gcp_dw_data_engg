/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_performance_rating_box_score AS SELECT
    ref_performance_rating_box_score.performance_rating_id,
    ref_performance_rating_box_score.performance_potential_id,
    ref_performance_rating_box_score.overall_performance_rating_desc,
    ref_performance_rating_box_score.performance_potential_desc,
    ref_performance_rating_box_score.box_score_num,
    ref_performance_rating_box_score.box_score_desc,
    ref_performance_rating_box_score.box_score_group_desc,
    ref_performance_rating_box_score.source_system_code,
    ref_performance_rating_box_score.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_performance_rating_box_score
;
