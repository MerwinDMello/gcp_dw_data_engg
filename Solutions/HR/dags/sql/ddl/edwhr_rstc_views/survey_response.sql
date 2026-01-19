/*==============================================================*/
/* Table: Survey_Response                                       */
/*==============================================================*/
/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.survey_response AS SELECT
    a.survey_question_sid,
    a.respondent_id,
    a.survey_receive_date,
    a.survey_mode_code,
    a.survey_response_sid,
    a.response_value_text,
    a.response_comment_text,
    a.survey_form_text,
    a.company_code,
    a.coid,
    a.patient_discharge_date,
    a.time_name_child,
    a.cms_submit_preliminary_ind,
    a.cms_submit_ind,
    a.adjusted_sample_ind,
    a.top_box_score_num,
    a.final_record_ind,
    a.vendor_assigned_unit_text,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.survey_response AS a
  WHERE upper(a.source_system_code) <> 'H'
;
