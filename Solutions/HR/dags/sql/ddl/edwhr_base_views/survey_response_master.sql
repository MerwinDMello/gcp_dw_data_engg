/*edwhr_base_views.survey_response_master*/
/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.survey_response_master AS SELECT
    survey_response_master.survey_response_sid,
    survey_response_master.eff_from_date,
    survey_response_master.survey_question_sid,
    survey_response_master.response_value_text,
    survey_response_master.response_label_text,
    survey_response_master.response_label_extended_text,
    survey_response_master.eff_to_date,
    survey_response_master.source_system_code,
    survey_response_master.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.survey_response_master
;