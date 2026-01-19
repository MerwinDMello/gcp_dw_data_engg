CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS SELECT
    candidate_answer.answer_sid,
    candidate_answer.valid_from_date,
    candidate_answer.answer_num,
    candidate_answer.valid_to_date,
    candidate_answer.answer_desc,
    candidate_answer.question_sid,
    candidate_answer.source_system_code,
    candidate_answer.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.candidate_answer
;