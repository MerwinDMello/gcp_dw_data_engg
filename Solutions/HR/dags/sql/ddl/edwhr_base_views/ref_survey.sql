CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_survey AS SELECT
    ref_survey.survey_sid,
    ref_survey.eff_from_date,
    ref_survey.survey_category_num,
    ref_survey.survey_category_code,
    ref_survey.survey_category_text,
    ref_survey.eff_to_date,
    ref_survey.survey_group_text,
    ref_survey.survey_date,
    ref_survey.survey_start_date,
    ref_survey.survey_end_date,
    ref_survey.source_system_code,
    ref_survey.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_survey
;