/*==============================================================*/
/* Table: Survey_Response_Master                                */
/*==============================================================*/
/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.survey_response_master AS SELECT
    a.survey_response_sid,
    a.eff_from_date,
    a.survey_question_sid,
    a.response_value_text,
    a.response_label_text,
    a.response_label_extended_text,
    a.eff_to_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.survey_response_master AS a
  WHERE upper(a.source_system_code) <> 'H'
;
