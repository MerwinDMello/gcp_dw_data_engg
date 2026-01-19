/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.ref_survey_type AS SELECT
    ref_survey_type.survey_type_code,
    ref_survey_type.survey_type_desc,
    ref_survey_type.source_system_code,
    ref_survey_type.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_survey_type
;