/*==============================================================*/
/* Table: Ref_Survey_Form                                       */
/*==============================================================*/
/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_survey_form AS SELECT
    a.survey_form_text,
    a.survey_form_name,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_survey_form AS a
;
