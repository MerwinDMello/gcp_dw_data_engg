/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.candidate_answer AS SELECT
      a.answer_sid,
      a.valid_from_date,
      a.answer_num,
      a.valid_to_date,
      a.answer_desc,
      a.question_sid,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.candidate_answer AS a
  ;

