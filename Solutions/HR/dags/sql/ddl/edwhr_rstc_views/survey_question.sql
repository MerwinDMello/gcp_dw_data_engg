/*==============================================================*/
/* Table: Survey_Question                                       */
/*==============================================================*/
/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.survey_question AS SELECT
    a.survey_question_sid,
    a.eff_from_date,
    a.survey_sid,
    a.survey_sub_category_text,
    a.base_question_id,
    a.question_id,
    a.question_type_code,
    a.question_short_name,
    a.question_desc,
    a.question_seq_num,
    a.top_box_num,
    a.top_box_high_num,
    a.measure_id_text,
    a.legacy_question_id,
    a.standard_flag,
    a.ignore_value,
    a.eff_to_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.survey_question AS a
  WHERE upper(a.source_system_code) <> 'H'
;
