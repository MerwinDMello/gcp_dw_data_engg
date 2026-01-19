CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.survey_question AS SELECT
    survey_question.survey_question_sid,
    survey_question.eff_from_date,
    survey_question.survey_sid,
    survey_question.survey_sub_category_text,
    survey_question.base_question_id,
    survey_question.question_id,
    survey_question.question_type_code,
    survey_question.question_short_name,
    survey_question.question_desc,
    survey_question.question_seq_num,
    survey_question.top_box_num,
    survey_question.top_box_high_num,
    survey_question.measure_id_text,
    survey_question.legacy_question_id,
    survey_question.standard_flag,
    survey_question.ignore_value,
    survey_question.eff_to_date,
    survey_question.source_system_code,
    survey_question.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.survey_question
;