CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_candidate_question_type
AS SELECT
    ref_candidate_question_type.question_type_num,
    ref_candidate_question_type.question_type_desc,
    ref_candidate_question_type.source_system_code,
    ref_candidate_question_type.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.ref_candidate_question_type;