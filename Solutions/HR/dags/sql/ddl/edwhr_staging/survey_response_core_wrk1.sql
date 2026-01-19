CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.survey_response_core_wrk1 (
survey_question_sid INT64 NOT NULL
, survey_response_sid INT64 NOT NULL
, respondent_id NUMERIC NOT NULL
, survey_receive_date DATE NOT NULL
, survey_mode_code STRING NOT NULL
, response_value_text STRING NOT NULL
, survey_form_text STRING
, company_code STRING
, coid STRING
, patient_discharge_date DATE
, time_name_child DATE
, cms_submit_preliminary_ind STRING
, cms_submit_ind STRING
, adjusted_sample_ind STRING
, top_box_score_num INT64
, final_record_ind STRING
, vendor_assigned_unit_text STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
, flag STRING
)
  ;
