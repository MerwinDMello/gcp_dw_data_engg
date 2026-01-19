/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.junc_student_exam AS SELECT
    a.student_sid,
    a.exam_id,
    a.valid_from_date,
    a.valid_to_date,
    a.exam_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.junc_student_exam AS a
;
