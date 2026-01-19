CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.candidate_question AS SELECT
    candidate_question.question_sid,
    candidate_question.valid_from_date,
    candidate_question.question_num,
    candidate_question.valid_to_date,
    candidate_question.creation_date,
    candidate_question.question_desc,
    candidate_question.question_code,
    candidate_question.last_modified_date,
    candidate_question.requisition_num,
    candidate_question.question_type_num,
    candidate_question.source_system_code,
    candidate_question.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.candidate_question
;