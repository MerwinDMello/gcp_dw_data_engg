CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.question_legacy_survey_xwalk AS SELECT
    question_legacy_survey_xwalk.question_id,
    question_legacy_survey_xwalk.measure_id_text,
    question_legacy_survey_xwalk.legacy_question_id,
    question_legacy_survey_xwalk.domain_id,
    question_legacy_survey_xwalk.domain_group_id,
    question_legacy_survey_xwalk.source_system_code,
    question_legacy_survey_xwalk.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.question_legacy_survey_xwalk
;