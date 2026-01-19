/*==============================================================*/
/* Table: Employee_Engagement_Question_Index                    */
/*==============================================================*/
/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.employee_engagement_question_index AS SELECT
    a.survey_question_sid,
    a.eff_from_date,
    a.index_question_id,
    a.survey_sid,
    a.question_id,
    a.survey_category_code,
    a.survey_year_num,
    a.eff_to_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.employee_engagement_question_index AS a
  WHERE upper(a.source_system_code) <> 'H'
;
