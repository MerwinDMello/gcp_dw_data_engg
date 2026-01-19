CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.respondent_detail AS SELECT
    respondent_detail.respondent_id,
    respondent_detail.survey_receive_date,
    respondent_detail.respondent_type_code,
    respondent_detail.survey_sid,
    respondent_detail.respondent_3_4_id,
    respondent_detail.employee_num,
    respondent_detail.source_system_code,
    respondent_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.respondent_detail
;