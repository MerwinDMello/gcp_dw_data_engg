/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.employee_engagement_question_index AS SELECT
    employee_engagement_question_index.survey_question_sid,
    employee_engagement_question_index.eff_from_date,
    employee_engagement_question_index.index_question_id,
    employee_engagement_question_index.survey_sid,
    employee_engagement_question_index.question_id,
    employee_engagement_question_index.survey_category_code,
    employee_engagement_question_index.survey_year_num,
    employee_engagement_question_index.eff_to_date,
    employee_engagement_question_index.source_system_code,
    employee_engagement_question_index.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_engagement_question_index
;