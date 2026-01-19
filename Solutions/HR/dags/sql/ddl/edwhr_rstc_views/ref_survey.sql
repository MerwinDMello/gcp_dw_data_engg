/*==============================================================*/
/* Table: Ref_Survey                                            */
/*==============================================================*/
/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_survey AS SELECT
    a.survey_sid,
    a.eff_from_date,
    a.survey_category_num,
    a.survey_category_code,
    a.survey_category_text,
    a.eff_to_date,
    a.survey_group_text,
    a.survey_date,
    a.survey_start_date,
    a.survey_end_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_survey AS a
  WHERE upper(a.source_system_code) <> 'H'
;
