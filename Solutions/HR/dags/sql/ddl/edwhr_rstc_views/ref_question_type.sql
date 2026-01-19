/*==============================================================*/
/* Table: Ref_Question_Type                                     */
/*==============================================================*/
/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_question_type AS SELECT
    a.question_type_code,
    a.question_type_desc,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_question_type AS a
;
