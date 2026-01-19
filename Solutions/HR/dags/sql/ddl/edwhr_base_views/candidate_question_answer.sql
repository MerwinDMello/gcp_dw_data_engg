CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.candidate_question_answer
AS SELECT
    candidate_question_answer.question_answer_sid,
    candidate_question_answer.valid_from_date,
    candidate_question_answer.question_answer_num,
    candidate_question_answer.candidate_sid,
    candidate_question_answer.valid_to_date,
    candidate_question_answer.creation_date,
    candidate_question_answer.question_sid,
    candidate_question_answer.answer_sid,
    candidate_question_answer.comment_text,
    candidate_question_answer.source_system_code,
    candidate_question_answer.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.candidate_question_answer;