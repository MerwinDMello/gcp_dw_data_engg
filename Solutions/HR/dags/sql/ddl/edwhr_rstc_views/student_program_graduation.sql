/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.student_program_graduation AS SELECT
    a.student_sid,
    a.nursing_program_id,
    a.valid_from_date,
    a.graduation_date,
    a.cumulative_gpa,
    a.nursing_school_id,
    a.campus_id,
    a.valid_to_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.student_program_graduation AS a
;
