/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.candidate_question_answer AS SELECT
      a.question_answer_sid,
      a.valid_from_date,
      a.question_answer_num,
      a.candidate_sid,
      a.valid_to_date,
      a.creation_date,
      a.question_sid,
      a.answer_sid,
      a.comment_text,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.candidate_question_answer AS a
  ;

