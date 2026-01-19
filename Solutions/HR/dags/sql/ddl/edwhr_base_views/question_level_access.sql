/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.question_level_access AS SELECT
    question_level_access.question_id,
    question_level_access.survey_category_num,
    question_level_access.survey_category_code,
    question_level_access.survey_sub_category_text,
    question_level_access.question_desc,
    question_level_access.question_type_desc,
    question_level_access.question_short_name,
    question_level_access.lob_name,
    question_level_access.source_system_code,
    question_level_access.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.question_level_access
;