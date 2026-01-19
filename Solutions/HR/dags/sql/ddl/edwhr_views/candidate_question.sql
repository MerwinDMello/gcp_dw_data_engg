/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.candidate_question AS SELECT
      a.question_sid,
      a.valid_from_date,
      a.question_num,
      a.valid_to_date,
      a.creation_date,
      a.question_desc,
      a.question_code,
      a.last_modified_date,
      a.requisition_num,
      a.question_type_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.candidate_question AS a
  ;

