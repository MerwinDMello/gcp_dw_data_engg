CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.survey_response AS SELECT
    survey_response.survey_question_sid,
    survey_response.respondent_id,
    survey_response.survey_receive_date,
    survey_response.survey_mode_code,
    survey_response.survey_response_sid,
    survey_response.response_value_text,
    survey_response.response_comment_text,
    survey_response.survey_form_text,
    survey_response.company_code,
    survey_response.coid,
    survey_response.patient_discharge_date,
    survey_response.time_name_child,
    survey_response.cms_submit_preliminary_ind,
    survey_response.cms_submit_ind,
    survey_response.adjusted_sample_ind,
    survey_response.top_box_score_num,
    survey_response.final_record_ind,
    survey_response.vendor_assigned_unit_text,
    survey_response.source_system_code,
    survey_response.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.survey_response
;